/*
 * server.js — the whole Lessonlab backend, meant to run on the teacher's own
 * laptop for one lesson at a time. Replaces both the earlier Firebase and
 * CloudKit backends with something much simpler: a single Node process that
 * (1) serves the teacher/student pages as plain static files, and (2) holds
 * one big in-memory JSON tree — exactly the shape Firebase's Realtime
 * Database used — that clients subscribe to over a WebSocket and get PUSHED
 * updates on, the instant something changes. No polling, no record schemas,
 * no query length limits: the server just IS the tree.
 *
 * WHY THIS BEATS CLOUDKIT FOR THIS USE CASE:
 *  - Real push, not polling — updates land in milliseconds, not 1.5-3s.
 *  - No CKRecord/field-size limits, no query comparator quirks, no schema
 *    deploys. A write is just "mutate this JS object, then tell watchers."
 *  - Runs entirely on hardware you already own — no account, no API token,
 *    no per-request cost.
 *
 * WHY THIS NEEDS A TUNNEL ON RESTRICTIVE WI-FI:
 *  - School Wi-Fi with client/AP isolation blocks devices from reaching
 *    each other directly on the LOCAL network, full stop — regardless of
 *    protocol or port. This server works around that the same way Firebase
 *    and CloudKit did: every device (including the teacher's own browser)
 *    reaches it by going OUT to the internet and back in, via a public
 *    https:// address from a tunnel (Cloudflare Tunnel, ngrok, etc.), not by
 *    trying to talk sideways to another device on the same Wi-Fi.
 *
 * PERSISTENCE: the in-memory tree is written to ./data.json on a short
 * debounce after every change, and reloaded from that file on startup. So
 * restarting the server (e.g. reopening your laptop the next day) picks up
 * where you left off — "saves to the teacher's device" in the most literal
 * sense: it's a file right next to this script.
 *
 * RUNNING THIS:
 *   1. npm install          (one-time, installs the "ws" package)
 *   2. node server.js       (starts on port 8080 by default)
 *   3. In another terminal: cloudflared tunnel --url http://localhost:8080
 *      (or your own tunnel tool) — this prints a https://*.trycloudflare.com
 *      address. Open THAT address (not localhost) on the teacher's device,
 *      and give the SAME address to students for student-ws.html.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const DATA_FILE = path.join(__dirname, 'data.json');
const PUBLIC_DIR = path.join(__dirname, 'public');

// ---------------------------------------------------------------------------
// The tree: one big nested JS object, exactly like a Firebase RTDB export.
// ---------------------------------------------------------------------------
let tree = {};
try {
  if (fs.existsSync(DATA_FILE)) {
    tree = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    console.log('[server] loaded existing data.json (' + Object.keys(tree).length + ' top-level keys)');
  }
} catch (e) {
  console.warn('[server] could not read data.json, starting empty:', e.message);
}

let saveTimer = null;
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    fs.writeFile(DATA_FILE, JSON.stringify(tree), (err) => {
      if (err) console.warn('[server] failed to save data.json:', err.message);
    });
  }, 300);
}

function splitPath(p) {
  return String(p || '').split('/').filter(Boolean);
}

function getAt(root, parts) {
  let node = root;
  for (const k of parts) {
    if (node == null || typeof node !== 'object') return undefined;
    node = node[k];
  }
  return node;
}

function setAt(root, parts, value) {
  if (parts.length === 0) {
    // Setting the whole root: replace contents in place (callers hold a
    // reference to `tree`, so we can't just reassign the variable here).
    for (const k of Object.keys(root)) delete root[k];
    if (value && typeof value === 'object') Object.assign(root, value);
    return;
  }
  let node = root;
  for (let i = 0; i < parts.length - 1; i++) {
    const k = parts[i];
    if (node[k] == null || typeof node[k] !== 'object') node[k] = {};
    node = node[k];
  }
  const last = parts[parts.length - 1];
  if (value === null || value === undefined) {
    delete node[last];
  } else {
    node[last] = value;
  }
}

function deepClone(v) {
  return v === undefined ? undefined : JSON.parse(JSON.stringify(v));
}

// ---------------------------------------------------------------------------
// Static file server for the teacher/student pages and the client shim.
// ---------------------------------------------------------------------------
const MIME = { '.html': 'text/html', '.js': 'application/javascript', '.json': 'application/json', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml', '.webmanifest': 'application/manifest+json' };

const httpServer = http.createServer((req, res) => {
  let urlPath = decodeURIComponent((req.url || '/').split('?')[0]);
  if (urlPath === '/') urlPath = '/index.html';
  const filePath = path.join(PUBLIC_DIR, urlPath);
  if (!filePath.startsWith(PUBLIC_DIR)) { res.writeHead(403); res.end('Forbidden'); return; }
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found: ' + urlPath); return; }
    const ext = path.extname(filePath);
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

// ---------------------------------------------------------------------------
// WebSocket protocol:
//   client -> server: {op:'sub',   id, path}            start watching a path
//   client -> server: {op:'unsub', id}                   stop watching
//   client -> server: {op:'get',   reqId, path}          one-off read
//   client -> server: {op:'set',   path, value}          overwrite a path
//   client -> server: {op:'update',path, patch}          merge keys at a path
//   client -> server: {op:'onDisconnectSet', path, value} fire this set when this socket closes
//   client -> server: {op:'onDisconnectCancel'}          cancel the above
//   server -> client: {op:'val',  id, value}             pushed value for a subscription
//   server -> client: {op:'val1', reqId, value}          one-off read reply
// ---------------------------------------------------------------------------
const wss = new WebSocketServer({ server: httpServer });

// subs: Map<ws, Map<subId, {path, lastSerialised}>>
const allSockets = new Set();

function notifySubscribers(changedPath) {
  // changedPath affects any subscription whose own path is an ancestor of,
  // descendant of, or equal to it. Simplest correct approach at classroom
  // scale: just recheck every open subscription's current value.
  for (const ws of allSockets) {
    const subs = ws.__subs;
    if (!subs) continue;
    for (const [id, sub] of subs) {
      const val = deepClone(getAt(tree, splitPath(sub.path)));
      const serialised = JSON.stringify(val === undefined ? null : val);
      if (serialised !== sub.lastSerialised) {
        sub.lastSerialised = serialised;
        safeSend(ws, { op: 'val', id, value: val === undefined ? null : val });
      }
    }
  }
}

function safeSend(ws, obj) {
  if (ws.readyState === ws.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (_) {}
  }
}

wss.on('connection', (ws) => {
  ws.__subs = new Map();
  ws.__onDisconnect = null;
  allSockets.add(ws);

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }

    if (msg.op === 'sub') {
      const parts = splitPath(msg.path);
      const val = deepClone(getAt(tree, parts));
      const serialised = JSON.stringify(val === undefined ? null : val);
      ws.__subs.set(msg.id, { path: msg.path, lastSerialised: serialised });
      safeSend(ws, { op: 'val', id: msg.id, value: val === undefined ? null : val });
      return;
    }
    if (msg.op === 'unsub') {
      ws.__subs.delete(msg.id);
      return;
    }
    if (msg.op === 'get') {
      const val = deepClone(getAt(tree, splitPath(msg.path)));
      safeSend(ws, { op: 'val1', reqId: msg.reqId, value: val === undefined ? null : val });
      return;
    }
    if (msg.op === 'set') {
      setAt(tree, splitPath(msg.path), msg.value);
      scheduleSave();
      notifySubscribers(msg.path);
      return;
    }
    if (msg.op === 'update') {
      const parts = splitPath(msg.path);
      const patch = msg.patch || {};
      for (const k of Object.keys(patch)) {
        setAt(tree, parts.concat(k), patch[k]);
      }
      scheduleSave();
      notifySubscribers(msg.path);
      return;
    }
    if (msg.op === 'onDisconnectSet') {
      ws.__onDisconnect = { path: msg.path, value: msg.value };
      return;
    }
    if (msg.op === 'onDisconnectCancel') {
      ws.__onDisconnect = null;
      return;
    }
  });

  ws.on('close', () => {
    allSockets.delete(ws);
    if (ws.__onDisconnect) {
      const { path: p, value } = ws.__onDisconnect;
      setAt(tree, splitPath(p), value);
      scheduleSave();
      notifySubscribers(p);
    }
  });
});

httpServer.listen(PORT, () => {
  console.log('[server] Lessonlab server running at http://localhost:' + PORT);
  console.log('[server] Run a tunnel (e.g. `cloudflared tunnel --url http://localhost:' + PORT + '`) and use the https address it prints instead of localhost, on every device including this one.');
});
