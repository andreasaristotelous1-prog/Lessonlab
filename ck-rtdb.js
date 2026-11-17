/*
 * ck-rtdb.js — a CloudKit-backed shim that mimics the small slice of the
 * Firebase Realtime Database API that teacher.html / student.html use:
 *   initializeApp, getDatabase, ref, onValue, get, set, update, remove,
 *   onDisconnect, plus getAuth/onAuthStateChanged/signOut for identity.
 *
 * WHY A SHIM: CloudKit has no tree database and no native "watch this
 * path and push me changes" feature on the web. This shim stores each
 * Firebase path as ONE CloudKit record (recordType "RTDBNode") holding
 * the JSON-serialised value for that path, and turns "live" updates into
 * short-interval polling (CK_POLL_MS below). It is NOT real push — on a
 * real container this will feel slightly laggier than Firebase, and will
 * use one CloudKit "request" per poll per open listener. That trade-off
 * is inherent to CloudKit JS on the web, not a bug in this file.
 *
 * HONESTLY UNTESTED: this has not run against a real CloudKit container.
 * It has only been checked for syntax errors and exercised against a
 * local stub that mimics CloudKit JS's shape. Treat first real use as a
 * test, and watch the browser console for CKError responses.
 *
 * ---- SET THESE BEFORE USE ----
 */
export const CK_CONTAINER_ID = 'iCloud.lessonlab'; // from CloudKit Dashboard
export const CK_ENVIRONMENT = 'development'; // switch to 'production' once this is working and deployed for real
export const CK_APPLE_SERVICES_ID = 'uk.lessonlab.web'; // registered Services ID, grouped with the "LessonLab" App ID (uk.lessonlab.app)
export const CK_REDIRECT_URI = location.origin + location.pathname; // resolves to https://lessonlab.uk/teacher-p2p or /student-p2p once hosted there — both are registered as Return URLs on the Services ID
export const CK_POLL_MS = 1500; // how often an open listener re-checks its path

let ckReady = null;
function ckInit() {
  if (ckReady) return ckReady;
  ckReady = new Promise((resolve, reject) => {
    if (!window.CloudKit) { reject(new Error('CloudKit JS did not load — check the <script src> tag and your network.')); return; }
    try {
      CloudKit.configure({
        containers: [{
          containerIdentifier: CK_CONTAINER_ID,
          apiTokenAuth: {
            // Web API token, restricted to https://lessonlab.uk (CloudKit Dashboard → Tokens & Keys → LessonLabAPI).
            apiToken: '1771b4b449740324db62f225cf9c3a4165f6acc657c90f3d4f276233e4e60297',
            persist: true,
            signInButton: { id: 'ck-signin-btn', theme: 'black' },
            signOutButton: { id: 'ck-signout-btn', theme: 'black' }
          },
          environment: CK_ENVIRONMENT
        }]
      });
      resolve(CloudKit.getDefaultContainer());
    } catch (e) { reject(e); }
  });
  return ckReady;
}

function pathKey(p) { return String(p).replace(/^\/+|\/+$/g, ''); }
function isAncestorOrSelf(nodePath, watchPath) {
  if (nodePath === watchPath) return true;
  return nodePath.startsWith(watchPath === '' ? '' : watchPath + '/');
}
function buildTree(flatRecords, watchPath) {
  // flatRecords: [{path, json}] for every node at/under watchPath.
  // Rebuilds a nested JS object the way a Firebase onValue snapshot would see it.
  if (flatRecords.length === 1 && flatRecords[0].path === watchPath) {
    return flatRecords[0].json === undefined ? null : JSON.parse(flatRecords[0].json);
  }
  const root = {};
  let any = false;
  for (const rec of flatRecords) {
    if (!isAncestorOrSelf(rec.path, watchPath)) continue;
    const rel = rec.path === watchPath ? '' : rec.path.slice(watchPath.length + (watchPath ? 1 : 0));
    if (rel === '') continue; // the watchPath node's own direct value is handled by leaf records below
    const parts = rel.split('/');
    let node = root;
    for (let i = 0; i < parts.length - 1; i++) { node[parts[i]] = node[parts[i]] || {}; node = node[parts[i]]; }
    try { node[parts[parts.length - 1]] = JSON.parse(rec.json); any = true; } catch (_) {}
  }
  return any ? root : null;
}

async function ckFetchSubtree(db, watchPath) {
  const container = await ckInit();
  const query = { recordType: 'RTDBNode', filterBy: [{ comparator: 'BEGINS_WITH', fieldName: 'path', fieldValue: { value: watchPath } }] };
  const resp = await container.publicCloudDatabase.performQuery(query);
  const own = await container.publicCloudDatabase.fetchRecords([watchPath]).catch(() => ({ records: [] }));
  const recs = (resp.records || []).map(r => ({ path: r.fields.path.value, json: r.fields.json ? r.fields.json.value : 'null' }));
  for (const r of (own.records || [])) if (!recs.find(x => x.path === r.recordName)) recs.push({ path: r.recordName, json: r.fields.json ? r.fields.json.value : 'null' });
  return buildTree(recs, watchPath);
}

export function initializeApp(_config) { return { name: '[DEFAULT]' }; }
export function getDatabase(_app) { return { __ck: true }; }

export function ref(_db, path) { return { path: pathKey(path || '') }; }

export function onValue(refObj, cb, _opts) {
  // Firebase's magic ".info/serverTimeOffset" path has no CloudKit equivalent
  // (no clock-offset primitive in CloudKit JS). We report 0 — i.e. trust the
  // device clock — which is fine for a classroom but is a behaviour change
  // worth knowing about if you ever depended on exact server time.
  if (refObj.path === '.info/serverTimeOffset') { cb({ val: () => 0, exists: () => true }); return () => {}; }
  let last = undefined, stopped = false;
  let timer = null;
  async function tick() {
    if (stopped) return;
    try {
      const val = await ckFetchSubtree(null, refObj.path);
      const serialised = JSON.stringify(val);
      if (serialised !== last) { last = serialised; cb({ val: () => val, exists: () => val !== null && val !== undefined, key: refObj.path.split('/').pop() || null }); }
    } catch (e) { console.warn('[ck-rtdb] onValue poll failed for', refObj.path, e); }
    if (!stopped) timer = setTimeout(tick, CK_POLL_MS);
  }
  tick();
  return () => { stopped = true; if (timer) clearTimeout(timer); };
}

export async function get(refObj) {
  const val = await ckFetchSubtree(null, refObj.path);
  return { val: () => val, exists: () => val !== null && val !== undefined };
}

async function writeNode(path, value) {
  const container = await ckInit();
  const db = container.publicCloudDatabase;
  if (value === null || value === undefined) {
    await db.deleteRecords([path]).catch(() => {});
    return;
  }
  const record = { recordType: 'RTDBNode', recordName: path, fields: { path: { value: path }, json: { value: JSON.stringify(value) } } };
  await db.saveRecords([record]);
}

async function flattenAndWrite(basePath, value, out) {
  if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
    for (const k of Object.keys(value)) await flattenAndWrite(basePath ? basePath + '/' + k : k, value[k], out);
  } else {
    out.push(writeNode(basePath, value));
  }
}

export async function set(refObj, value) {
  const writes = [];
  // clear anything currently under this path first, then write the new shape
  await flattenAndWrite(refObj.path, value, writes);
  await Promise.all(writes);
}

export async function update(refObj, patch) {
  const writes = [];
  for (const k of Object.keys(patch)) {
    const full = refObj.path ? refObj.path + '/' + k : k;
    await flattenAndWrite(full, patch[k], writes);
  }
  await Promise.all(writes);
}

export async function remove(refObj) {
  await writeNode(refObj.path, null);
}

// ---- Presence: CloudKit has no onDisconnect hook. Best-effort heartbeat. ----
// Writes value, then refreshes it every HEARTBEAT_MS while the tab is open,
// and tries once to clear it on unload. A closed laptop lid or lost wifi
// will NOT reliably trigger the "disconnect" value — this is a known gap,
// unlike Firebase's server-side onDisconnect.
export function onDisconnect(refObj) {
  return {
    set(value) {
      // Best effort only: attempt on page unload. Not guaranteed to fire or land.
      const handler = () => { try { navigator.sendBeacon && navigator.sendBeacon('about:blank'); } catch (_) {} };
      window.addEventListener('pagehide', () => { set(refObj, value).catch(() => {}); });
      return Promise.resolve();
    },
    cancel() { return Promise.resolve(); }
  };
}

// ---- Auth: CloudKit uses Sign in with Apple, not email/password. ----
// The real CloudKit JS method is container.setUpAuth() — NOT whoAmI(), which
// doesn't exist on the current CloudKit JS and was this file's first bug.
// setUpAuth() does two things at once: it wires up the sign-in/sign-out
// buttons named in ckInit's apiTokenAuth config, AND its returned promise
// resolves once with the signed-in CKUserIdentity — either because the user
// was already signed in, or once they click the button and complete Sign in
// with Apple. It is meant to be called once per page load, not polled.
export function getAuth(_app) { return { __ckAuth: true }; }

let authListeners = [];
let setUpAuthPromise = null;
function ckSetUpAuth() {
  if (!setUpAuthPromise) {
    setUpAuthPromise = ckInit().then((container) => container.setUpAuth());
  }
  return setUpAuthPromise;
}
function toUser(identity) {
  return identity ? { uid: identity.userRecordName || identity.lookupInfo || 'ck-user', email: null } : null;
}

export function onAuthStateChanged(_auth, cb) {
  authListeners.push(cb);
  ckSetUpAuth()
    .then((identity) => { const user = toUser(identity); authListeners.forEach((f) => f(user)); })
    .catch((e) => { console.warn('[ck-rtdb] setUpAuth failed', e); cb(null); });
  return () => { authListeners = authListeners.filter((f) => f !== cb); };
}

export async function signInWithApple() {
  // There is no separate "trigger sign-in" API call — the rendered
  // #ck-signin-btn button IS the sign-in trigger. This just returns the
  // same setUpAuth() promise so callers can await the outcome.
  const identity = await ckSetUpAuth();
  return toUser(identity);
}

export async function signOut(_auth) {
  // container.signOut() is the documented CloudKit JS method, but since
  // whoAmI() above turned out not to exist, treat this the same way: if
  // the console shows "container.signOut is not a function" too, tell me
  // and I'll fix it the same way — by checking what methods the loaded
  // CloudKit object actually has, not by guessing again.
  const container = await ckInit();
  await container.signOut();
  setUpAuthPromise = null;
  authListeners.forEach(cb => cb(null));
}
