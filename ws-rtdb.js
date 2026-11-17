/*
 * ws-rtdb.js — client-side shim presenting the same small Firebase Realtime
 * Database API surface that teacher.html / student.html use (initializeApp,
 * getDatabase, ref, onValue, get, set, update, remove, onDisconnect), but
 * backed by a single shared WebSocket to server.js instead of Firebase or
 * CloudKit. There is no polling here — the server pushes a new value the
 * instant something relevant changes, which is why this feels fast.
 *
 * There is also no real authentication — server.js trusts any connection
 * (same as this app's original Firebase setup before CloudKit required
 * Sign in with Apple to write). getAuth/onAuthStateChanged/signInWithApple/
 * signOut are kept as no-op-ish stand-ins purely so teacher-ws.html and
 * student-ws.html — copies of the CloudKit pages — don't need every one of
 * their ~300 call sites rewritten. onAuthStateChanged fires immediately with
 * a small locally-generated "user" (a random id kept in localStorage), so
 * the app proceeds straight past any login screen.
 *
 * RECONNECTION: if the WebSocket drops (laptop sleeps, tunnel hiccups), this
 * reconnects automatically every 1.5s. While disconnected, each onValue
 * callback keeps serving the last known value it cached in localStorage —
 * so a student's screen doesn't go blank mid-lesson, it just stops updating
 * until the connection comes back.
 */

function wsUrl() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  return proto + '//' + location.host + '/';
}

let socket = null;
let socketReady = null;
let nextSubId = 1;
let nextReqId = 1;
const subs = new Map();      // id -> {path, cb}
const pendingGets = new Map(); // reqId -> resolve
let queuedOnDisconnect = null;

function cacheKey(path) { return 'ws-rtdb-cache:' + path; }
function readCache(path) {
  try { const raw = localStorage.getItem(cacheKey(path)); return raw === null ? undefined : JSON.parse(raw); } catch (_) { return undefined; }
}
function writeCache(path, value) {
  try { localStorage.setItem(cacheKey(path), JSON.stringify(value === undefined ? null : value)); } catch (_) {}
}

function connect() {
  if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return socketReady;
  socketReady = new Promise((resolve) => {
    const ws = new WebSocket(wsUrl());
    socket = ws;
    ws.addEventListener('open', () => {
      console.log('[ws-rtdb] connected');
      // Re-subscribe everything (fresh connection = fresh subscriptions).
      for (const [id, s] of subs) ws.send(JSON.stringify({ op: 'sub', id, path: s.path }));
      if (queuedOnDisconnect) ws.send(JSON.stringify({ op: 'onDisconnectSet', path: queuedOnDisconnect.path, value: queuedOnDisconnect.value }));
      resolve(ws);
    });
    ws.addEventListener('message', (ev) => {
      let msg;
      try { msg = JSON.parse(ev.data); } catch (_) { return; }
      if (msg.op === 'val') {
        const s = subs.get(msg.id);
        if (s) { writeCache(s.path, msg.value); s.cb(msg.value); }
      } else if (msg.op === 'val1') {
        const resolveFn = pendingGets.get(msg.reqId);
        if (resolveFn) { pendingGets.delete(msg.reqId); resolveFn(msg.value); }
      }
    });
    ws.addEventListener('close', () => {
      console.warn('[ws-rtdb] disconnected — retrying in 1.5s');
      setTimeout(connect, 1500);
    });
    ws.addEventListener('error', () => { try { ws.close(); } catch (_) {} });
  });
  return socketReady;
}
connect();

function send(obj) {
  connect().then((ws) => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); });
}

function pathKey(p) { return String(p || '').replace(/^\/+|\/+$/g, ''); }

export function initializeApp(_config) { return { name: '[DEFAULT]' }; }
export function getDatabase(_app) { return { __ws: true }; }
export function ref(_db, path) { return { path: pathKey(path || '') }; }

export function onValue(refObj, cb, _opts) {
  const id = nextSubId++;
  const cached = readCache(refObj.path);
  subs.set(id, { path: refObj.path, cb: (val) => cb({ val: () => val, exists: () => val !== null && val !== undefined, key: refObj.path.split('/').pop() || null }) });
  if (cached !== undefined) {
    // Show the last known value immediately, don't wait for the round trip —
    // this is what keeps a refreshed/reconnecting page from flashing blank.
    cb({ val: () => cached, exists: () => cached !== null && cached !== undefined, key: refObj.path.split('/').pop() || null });
  }
  send({ op: 'sub', id, path: refObj.path });
  return () => { subs.delete(id); send({ op: 'unsub', id }); };
}

export function get(refObj) {
  const reqId = nextReqId++;
  return new Promise((resolve) => {
    pendingGets.set(reqId, (val) => resolve({ val: () => val, exists: () => val !== null && val !== undefined }));
    send({ op: 'get', reqId, path: refObj.path });
  });
}

export function set(refObj, value) {
  writeCache(refObj.path, value);
  send({ op: 'set', path: refObj.path, value: value === undefined ? null : value });
  return Promise.resolve();
}

export function update(refObj, patch) {
  send({ op: 'update', path: refObj.path, patch });
  return Promise.resolve();
}

export function remove(refObj) {
  send({ op: 'set', path: refObj.path, value: null });
  return Promise.resolve();
}

export function onDisconnect(refObj) {
  return {
    set(value) {
      queuedOnDisconnect = { path: refObj.path, value };
      send({ op: 'onDisconnectSet', path: refObj.path, value });
      return Promise.resolve();
    },
    cancel() {
      queuedOnDisconnect = null;
      send({ op: 'onDisconnectCancel' });
      return Promise.resolve();
    }
  };
}

// ---- Auth stand-ins: no real identity system, so these just let the app
// proceed as if someone is always already signed in. ----
function getLocalUser() {
  let uid = localStorage.getItem('ws-rtdb-uid');
  if (!uid) { uid = 'local-' + Math.random().toString(36).slice(2) + Date.now().toString(36); localStorage.setItem('ws-rtdb-uid', uid); }
  return { uid, email: null };
}
export function getAuth(_app) { return { __wsAuth: true }; }
export function onAuthStateChanged(_auth, cb) { const user = getLocalUser(); setTimeout(() => cb(user), 0); return () => {}; }
export async function signInWithApple() { return getLocalUser(); }
export async function signOut(_auth) { /* no-op: nothing to sign out of */ }
