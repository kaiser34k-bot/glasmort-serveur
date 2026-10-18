/* ============================================================
   Serveur des comptes de Glasmort
   - comptes (pseudo + mot de passe, haché scrypt), sessions par jeton
   - progression (niveau, prestige, déblocages…) sauvegardée en ligne
   - amis (demandes, présence en ligne), groupes de 4 avec invitations
   - lancement de partie : l'hôte crée la partie pair-à-pair (PeerJS) et
     le serveur transmet le code aux autres membres (plus besoin de code)
   Stockage : fichier JSON (data/users.json) ou PostgreSQL si DATABASE_URL.
   Lancer : node server.js   (port : variable PORT, 8790 par défaut)
   ============================================================ */
'use strict';
const http = require('http'), crypto = require('crypto'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8790;
const MAX_PARTY = 4;
const GRACE_MS = 90 * 1000;            // un membre qui recharge (changement de carte) garde sa place
const MAPS = ['montouliers', 'train', 'marraines', 'cloche'];
const MAP_NAMES = { montouliers: 'Le Glas de Montouliers', train: 'Le Train des Âmes', marraines: 'La Forêt des Marraines', cloche: 'Le Royaume Sous la Cloche' };
const HEROES = ['gabriel', 'nael', 'pablo', 'samuel'];

/* ---------------- Stockage ---------------- */
// tous les comptes sont gardés en mémoire ; on écrit ceux qui ont changé (toutes les 2 s au plus)
const users = new Map();   // nom en minuscules -> compte
const dirty = new Set();
let store;
class FileStore {
  constructor(file) { this.file = file; }
  async load() {
    try { const d = JSON.parse(fs.readFileSync(this.file, 'utf8')); for (const k in d) users.set(k, d[k]); } catch (e) { if (e.code !== 'ENOENT') console.error('lecture', e.message); }
  }
  async save() {
    const o = {}; for (const [k, v] of users) o[k] = v;
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
    const tmp = this.file + '.tmp'; fs.writeFileSync(tmp, JSON.stringify(o)); fs.renameSync(tmp, this.file);
  }
}
class PgStore {
  constructor(url) {
    const { Pool } = require('pg'), local = /@(localhost|127\.0\.0\.1)[:/]/.test(url);
    // Neon & co : TLS avec certificat vérifié ; les paramètres sslmode / channel_binding de l'adresse sont gérés ici
    try { const u = new URL(url); u.searchParams.delete('sslmode'); u.searchParams.delete('channel_binding'); url = u.toString(); } catch (e) { }
    this.pool = new Pool({ connectionString: url, ssl: local ? false : { rejectUnauthorized: true }, max: 3, idleTimeoutMillis: 30000 });
    // la base gratuite s'endort et coupe les connexions inactives : on ne plante pas, la suivante se reconnecte
    this.pool.on('error', e => console.error('base de données :', e.message));
  }
  async load() {
    await this.pool.query('CREATE TABLE IF NOT EXISTS zv_users (name TEXT PRIMARY KEY, data JSONB NOT NULL)');
    const r = await this.pool.query('SELECT name, data FROM zv_users'); for (const row of r.rows) users.set(row.name, row.data);
  }
  async save(keys) {
    for (const k of keys) { const u = users.get(k); if (u) await this.pool.query('INSERT INTO zv_users (name, data) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET data = EXCLUDED.data', [k, u]); else await this.pool.query('DELETE FROM zv_users WHERE name = $1', [k]); }
  }
}
function markDirty(k) { dirty.add(k); }
let saving = false;
setInterval(async () => {
  if (saving || !dirty.size) return; saving = true; const keys = [...dirty]; dirty.clear();
  try { await store.save(keys); } catch (e) { console.error('sauvegarde', e.message); for (const k of keys) dirty.add(k); }
  saving = false;
}, 2000);

/* ---------------- Mots de passe et jetons ---------------- */
const scrypt = (pass, salt) => new Promise((res, rej) => crypto.scrypt(pass, salt, 64, { N: 16384, r: 8, p: 1 }, (e, k) => e ? rej(e) : res(k)));
async function hashPass(pass) { const salt = crypto.randomBytes(16); return { salt: salt.toString('hex'), hash: (await scrypt(pass, salt)).toString('hex') }; }
async function checkPass(u, pass) { const k = await scrypt(pass, Buffer.from(u.salt, 'hex')); const h = Buffer.from(u.hash, 'hex'); return h.length === k.length && crypto.timingSafeEqual(h, k); }
const sha = s => crypto.createHash('sha256').update(s).digest('hex');
function newToken(u) {
  const t = crypto.randomBytes(32).toString('hex');
  u.tokens = (u.tokens || []).filter(x => Date.now() - x.t < 1000 * 3600 * 24 * 120).slice(-9);
  u.tokens.push({ h: sha(t), t: Date.now() }); return t;
}
const NAME_RE = /^[A-Za-z0-9_\-]{3,16}$/;

/* ---------------- Limites anti-abus ---------------- */
const tries = new Map();   // ip -> [horodatages]
function limited(ip, n = 12, ms = 60000) {
  const now = Date.now(), a = (tries.get(ip) || []).filter(t => now - t < ms); a.push(now); tries.set(ip, a); return a.length > n;
}
setInterval(() => { const now = Date.now(); for (const [k, a] of tries) if (!a.some(t => now - t < 60000)) tries.delete(k); }, 60000);

/* ---------------- Présence ---------------- */
const online = new Map();  // nom -> { socks: Set, status, map, hero, cos, offT }
const parties = new Map(); // id -> groupe
const partyOf = new Map(); // nom -> id du groupe
const lc = s => String(s || '').toLowerCase();
function display(k) { const u = users.get(k); return u ? u.name : k; }
function isOn(k) { const o = online.get(k); return !!(o && o.socks.size); }
function send(ws, m) { if (ws.readyState === 1) ws.send(JSON.stringify(m)); }
function sendTo(k, m) { const o = online.get(k); if (o) for (const ws of o.socks) send(ws, m); }
function presence(k) {
  const o = online.get(k), p = partyOf.get(k), P = p && parties.get(p);
  return { name: display(k), online: isOn(k), status: o ? o.status : 'off', map: o ? o.map : null, party: P ? P.members.length : 0, inGame: !!(P && P.state === 'ingame') };
}
function meMsg(k) {
  const u = users.get(k); if (!u) return null;
  return { t: 'me', name: u.name, friends: (u.friends || []).map(presence).sort((a, b) => (b.online - a.online) || a.name.localeCompare(b.name)), reqIn: (u.reqIn || []).map(display), reqOut: (u.reqOut || []).map(display) };
}
function pushMe(k) { if (isOn(k)) sendTo(k, meMsg(k)); }
function pushFriends(k) { const u = users.get(k); if (u) for (const f of u.friends || []) pushMe(f); }

/* ---------------- Groupes ---------------- */
function partyView(P) {
  return {
    id: P.id, leader: display(P.leader), map: P.map, state: P.state,
    members: P.members.map(k => { const o = online.get(k) || {}; return { name: display(k), hero: P.heroes[k] || o.hero || null, cos: P.cos[k] || o.cos || null, online: isOn(k), ready: !!P.ready[k], leader: k === P.leader }; }),
    invited: [...P.invited.keys()].map(display), chat: P.chat.slice(-30)
  };
}
function pushParty(P) { const v = partyView(P); for (const k of P.members) sendTo(k, { t: 'party', party: v }); for (const k of P.members) pushFriends(k); }
function newParty(k) {
  const id = crypto.randomBytes(6).toString('hex'), o = online.get(k) || {};
  const P = { id, leader: k, members: [k], map: o.map && MAPS.includes(o.map) ? o.map : 'montouliers', state: 'lobby', code: null, invited: new Map(), chat: [], heroes: {}, cos: {}, ready: {} };
  if (o.hero) P.heroes[k] = o.hero; if (o.cos) P.cos[k] = o.cos;
  parties.set(id, P); partyOf.set(k, id); return P;
}
function leaveParty(k, why) {
  const id = partyOf.get(k), P = id && parties.get(id); partyOf.delete(k); if (!P) return;
  P.members = P.members.filter(m => m !== k); delete P.heroes[k]; delete P.cos[k]; delete P.ready[k];
  sendTo(k, { t: 'party', party: null });
  if (!P.members.length) { parties.delete(id); return; }
  if (P.leader === k) { P.leader = P.members.find(isOn) || P.members[0]; P.state = 'lobby'; P.code = null; sysChat(P, `${display(P.leader)} devient chef du groupe.`); }
  sysChat(P, `${display(k)} ${why || 'a quitté le groupe'}.`);
  // seul dans son groupe : le groupe disparaît
  if (P.members.length === 1 && !P.invited.size) { const last = P.members[0]; parties.delete(id); partyOf.delete(last); sendTo(last, { t: 'party', party: null }); pushFriends(last); }
  else { if (P.state !== 'ingame') { P.state = 'lobby'; P.code = null; } pushParty(P); }
  pushFriends(k);
}
function sysChat(P, text) { P.chat.push({ from: null, text, at: Date.now() }); if (P.chat.length > 60) P.chat.shift(); }

/* ---------------- Messages ---------------- */
const H = {};
// --- comptes ---
H.register = async (c, m) => {
  if (limited(c.ip)) throw 'Trop d’essais, attends une minute.';
  const name = String(m.name || '').trim(), pass = String(m.pass || '');
  if (!NAME_RE.test(name)) throw 'Pseudo : 3 à 16 caractères (lettres, chiffres, - et _).';
  if (pass.length < 6 || pass.length > 100) throw 'Mot de passe : 6 caractères minimum.';
  const k = lc(name); if (users.has(k)) throw 'Ce pseudo est déjà pris.';
  const u = Object.assign({ name, created: Date.now(), friends: [], reqIn: [], reqOut: [], prog: null, progT: 0 }, await hashPass(pass));
  users.set(k, u); markDirty(k);
  return login(c, k, newToken(u));
};
H.login = async (c, m) => {
  if (limited(c.ip)) throw 'Trop d’essais, attends une minute.';
  const k = lc(String(m.name || '').trim()), u = users.get(k);
  if (!u || !(await checkPass(u, String(m.pass || '')))) throw 'Pseudo ou mot de passe incorrect.';
  return login(c, k, newToken(u));
};
H.resume = async (c, m) => {
  if (limited(c.ip, 30)) throw 'Trop d’essais.';
  const k = lc(m.name), u = users.get(k), h = sha(String(m.token || ''));
  const t = u && (u.tokens || []).find(x => x.h === h);
  if (!t) throw 'Session expirée, reconnecte-toi.';
  t.t = Date.now(); markDirty(k);
  return login(c, k, m.token);
};
H.logout = async c => { const k = c.user; if (!k) return {}; const u = users.get(k); if (u && c.token) { u.tokens = (u.tokens || []).filter(x => x.h !== sha(c.token)); markDirty(k); } detach(c); return {}; };
H.passwd = async (c, m) => {
  const u = need(c); if (!(await checkPass(u, String(m.old || '')))) throw 'Ancien mot de passe incorrect.';
  const pass = String(m.pass || ''); if (pass.length < 6 || pass.length > 100) throw 'Mot de passe : 6 caractères minimum.';
  Object.assign(u, await hashPass(pass)); u.tokens = []; const token = newToken(u); markDirty(c.user); c.token = token; return { token };
};
// suppression du compte (mot de passe demandé) : amis, demandes et groupe nettoyés
H.deleteAccount = async (c, m) => {
  const u = need(c), k = c.user; if (!(await checkPass(u, String(m.pass || '')))) throw 'Mot de passe incorrect.';
  if (partyOf.has(k)) leaveParty(k);
  for (const [o, v] of users) if (o !== k) { const n0 = JSON.stringify([v.friends, v.reqIn, v.reqOut]); for (const f of ['friends', 'reqIn', 'reqOut']) v[f] = (v[f] || []).filter(x => x !== k); if (JSON.stringify([v.friends, v.reqIn, v.reqOut]) !== n0) { markDirty(o); pushMe(o); } }
  users.delete(k); markDirty(k); detach(c); online.delete(k); return {};
};
function need(c) { const u = c.user && users.get(c.user); if (!u) throw 'Connecte-toi d’abord.'; return u; }
function login(c, k, token) {
  if (c.user && c.user !== k) detach(c);
  c.user = k; c.token = token;
  let o = online.get(k); if (!o) online.set(k, o = { socks: new Set(), status: 'menu', map: null, hero: null, cos: null });
  o.socks.add(c.ws); o.offT = 0;
  const u = users.get(k); u.seen = Date.now(); markDirty(k);
  setTimeout(() => { pushMe(k); pushFriends(k); const id = partyOf.get(k), P = id && parties.get(id); sendTo(k, { t: 'party', party: P ? partyView(P) : null }); if (P) pushParty(P); }, 0);
  return { name: u.name, token, prog: u.prog, progT: u.progT || 0 };
}
function detach(c) {
  const k = c.user; if (!k) return; c.user = null;
  const o = online.get(k); if (!o) return; o.socks.delete(c.ws);
  if (o.socks.size) return;
  o.offT = Date.now(); pushFriends(k);
  const id = partyOf.get(k), P = id && parties.get(id); if (P) pushParty(P);
  setTimeout(() => { const q = online.get(k); if (q && !q.socks.size && Date.now() - q.offT >= GRACE_MS - 50) { if (partyOf.has(k)) leaveParty(k, 's’est déconnecté'); online.delete(k); pushFriends(k); } }, GRACE_MS);
}
// --- progression ---
H.progPut = async (c, m) => {
  const u = need(c), s = JSON.stringify(m.prog || null);
  if (s.length > 400000) throw 'Progression trop volumineuse.';
  u.prog = m.prog; u.progT = Date.now(); markDirty(c.user); return { progT: u.progT };
};
H.progGet = async c => { const u = need(c); return { prog: u.prog, progT: u.progT || 0 }; };
// --- présence (écran, carte, personnage choisi) ---
H.status = async (c, m) => {
  need(c); const o = online.get(c.user);
  if (m.status) o.status = String(m.status).slice(0, 12);
  if (MAPS.includes(m.map)) o.map = m.map;
  if (HEROES.includes(m.hero)) o.hero = m.hero;
  if (m.cos && typeof m.cos === 'object' && JSON.stringify(m.cos).length < 2000) o.cos = m.cos;
  const id = partyOf.get(c.user), P = id && parties.get(id);
  if (P) { if (o.hero) P.heroes[c.user] = o.hero; if (o.cos) P.cos[c.user] = o.cos; pushParty(P); } else pushFriends(c.user);
  return {};
};
// --- amis ---
H.friendAdd = async (c, m) => {
  const u = need(c), k = lc(m.name), f = users.get(k);
  if (!f) throw 'Aucun joueur avec ce pseudo.'; if (k === c.user) throw 'C’est toi !';
  if (u.friends.includes(k)) throw 'Vous êtes déjà amis.';
  if (u.reqIn.includes(k)) return H.friendAccept(c, m);   // il t'avait déjà demandé : accepté
  if (!u.reqOut.includes(k)) u.reqOut.push(k); if (!f.reqIn.includes(c.user)) f.reqIn.push(c.user);
  markDirty(c.user); markDirty(k); pushMe(c.user); pushMe(k);
  sendTo(k, { t: 'notice', kind: 'friendReq', from: u.name, text: `${u.name} veut t’ajouter en ami.` });
  return {};
};
H.friendAccept = async (c, m) => {
  const u = need(c), k = lc(m.name), f = users.get(k);
  if (!f || !u.reqIn.includes(k)) throw 'Pas de demande de ce joueur.';
  u.reqIn = u.reqIn.filter(x => x !== k); f.reqOut = f.reqOut.filter(x => x !== c.user);
  if (!u.friends.includes(k)) u.friends.push(k); if (!f.friends.includes(c.user)) f.friends.push(c.user);
  markDirty(c.user); markDirty(k); pushMe(c.user); pushMe(k);
  sendTo(k, { t: 'notice', text: `${u.name} a accepté ta demande d’ami.` });
  return {};
};
H.friendDecline = async (c, m) => {
  const u = need(c), k = lc(m.name), f = users.get(k);
  u.reqIn = u.reqIn.filter(x => x !== k); u.reqOut = u.reqOut.filter(x => x !== k);
  if (f) { f.reqOut = f.reqOut.filter(x => x !== c.user); f.reqIn = f.reqIn.filter(x => x !== c.user); markDirty(k); pushMe(k); }
  markDirty(c.user); pushMe(c.user); return {};
};
H.friendRemove = async (c, m) => {
  const u = need(c), k = lc(m.name), f = users.get(k);
  u.friends = u.friends.filter(x => x !== k); if (f) { f.friends = f.friends.filter(x => x !== c.user); markDirty(k); pushMe(k); }
  markDirty(c.user); pushMe(c.user); return {};
};
// --- groupe ---
function myParty(c) { const id = partyOf.get(c.user); return id ? parties.get(id) : null; }
function leaderOnly(c) { const P = myParty(c); if (!P) throw 'Tu n’es pas dans un groupe.'; if (P.leader !== c.user) throw 'Seul le chef du groupe peut faire ça.'; return P; }
H.partyCreate = async c => { need(c); let P = myParty(c); if (!P) P = newParty(c.user); pushParty(P); return {}; };
H.partyInvite = async (c, m) => {
  const u = need(c), k = lc(m.name);
  if (!users.has(k)) throw 'Aucun joueur avec ce pseudo.';
  if (!u.friends.includes(k)) throw 'Tu ne peux inviter que tes amis.';
  if (!isOn(k)) throw `${display(k)} n’est pas connecté.`;
  let P = myParty(c); if (!P) P = newParty(c.user);
  if (P.members.includes(k)) throw 'Déjà dans ton groupe.';
  if (P.members.length >= MAX_PARTY) throw 'Le groupe est complet (4 joueurs).';
  if (P.state === 'ingame') throw 'Le groupe est en pleine partie.';
  P.invited.set(k, Date.now()); pushParty(P);
  sendTo(k, { t: 'invite', id: P.id, from: u.name, map: P.map, n: P.members.length });
  setTimeout(() => { if (P.invited.get(k) && Date.now() - P.invited.get(k) >= 59000) { P.invited.delete(k); if (parties.has(P.id)) pushParty(P); } }, 60000);
  return {};
};
H.partyAccept = async (c, m) => {
  need(c); const P = parties.get(String(m.id || ''));
  if (!P || !P.invited.has(c.user)) throw 'Cette invitation a expiré.';
  if (P.members.length >= MAX_PARTY) throw 'Le groupe est complet.';
  if (P.state === 'ingame') throw 'Ils ont déjà lancé la partie.';
  if (partyOf.has(c.user)) leaveParty(c.user);
  P.invited.delete(c.user); P.members.push(c.user); partyOf.set(c.user, P.id);
  const o = online.get(c.user); if (o && o.hero) P.heroes[c.user] = o.hero; if (o && o.cos) P.cos[c.user] = o.cos;
  sysChat(P, `${display(c.user)} a rejoint le groupe.`); pushParty(P); return {};
};
H.partyDecline = async (c, m) => {
  need(c); const P = parties.get(String(m.id || '')); if (!P) return {};
  P.invited.delete(c.user); sendTo(P.leader, { t: 'notice', text: `${display(c.user)} a refusé l’invitation.` });
  if (P.members.length === 1 && !P.invited.size) { parties.delete(P.id); partyOf.delete(P.members[0]); sendTo(P.members[0], { t: 'party', party: null }); } else pushParty(P);
  return {};
};
H.partyLeave = async c => { need(c); leaveParty(c.user); return {}; };
H.partyKick = async (c, m) => { const P = leaderOnly(c), k = lc(m.name); if (k === c.user || !P.members.includes(k)) throw 'Joueur introuvable.'; leaveParty(k, 'a été exclu'); sendTo(k, { t: 'notice', text: 'Le chef t’a retiré du groupe.' }); return {}; };
H.partyPromote = async (c, m) => { const P = leaderOnly(c), k = lc(m.name); if (!P.members.includes(k)) throw 'Joueur introuvable.'; P.leader = k; sysChat(P, `${display(k)} devient chef du groupe.`); pushParty(P); return {}; };
H.partyMap = async (c, m) => { const P = leaderOnly(c); if (!MAPS.includes(m.map)) throw 'Carte inconnue.'; if (P.state !== 'lobby') throw 'Partie en cours.'; P.map = m.map; P.ready = {}; sysChat(P, `Carte : ${MAP_NAMES[m.map]}`); pushParty(P); return {}; };
H.partyReady = async (c, m) => { const P = myParty(c); if (!P) throw 'Pas de groupe.'; P.ready[c.user] = !!m.ready; pushParty(P); return {}; };
H.partyChat = async (c, m) => {
  const P = myParty(c); if (!P) throw 'Pas de groupe.'; if (limited('chat:' + c.user, 8, 5000)) throw 'Doucement !';
  const text = String(m.text || '').replace(/\s+/g, ' ').trim().slice(0, 200); if (!text) return {};
  P.chat.push({ from: display(c.user), text, at: Date.now() }); if (P.chat.length > 60) P.chat.shift(); pushParty(P); return {};
};
// le chef a créé la partie pair-à-pair : on transmet le code aux autres
H.partyLaunch = async (c, m) => {
  const P = leaderOnly(c), code = String(m.code || '').toUpperCase();
  if (!/^[A-Z0-9]{4,8}$/.test(code)) throw 'Code invalide.';
  const off = P.members.filter(k => !isOn(k)); if (off.length) throw `${off.map(display).join(', ')} n’est pas connecté.`;
  P.state = 'launching'; P.code = code; P.launchT = Date.now();
  for (const k of P.members) if (k !== c.user) sendTo(k, { t: 'launch', code, map: P.map, leader: display(P.leader) });
  pushParty(P); return {};
};
H.partyState = async (c, m) => {
  const P = leaderOnly(c); if (!['lobby', 'ingame'].includes(m.state)) throw 'État inconnu.';
  P.state = m.state; if (m.state === 'lobby') { P.code = null; P.ready = {}; }
  pushParty(P); return {};
};

/* ---------------- Serveur HTTP + WebSocket ---------------- */
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); return; }
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
  res.end(`Serveur Glasmort en ligne · ${users.size} comptes · ${[...online.values()].filter(o => o.socks.size).length} connectés · ${parties.size} groupes\n`);
});
const wss = new WebSocketServer({ server, maxPayload: 512 * 1024 });
wss.on('connection', (ws, req) => {
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const c = { ws, ip, user: null, token: null, alive: true };
  ws.on('pong', () => { c.alive = true; });
  ws.on('message', async raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    if (m.t === 'ping') return send(ws, { t: 'pong' });
    const fn = H[m.t]; let out;
    try { if (!fn) throw 'Commande inconnue.'; out = Object.assign({ ok: true }, await fn(c, m)); }
    catch (e) { out = { ok: false, err: typeof e === 'string' ? e : 'Erreur du serveur.' }; if (typeof e !== 'string') console.error(m.t, e); }
    if (m.rid) send(ws, Object.assign({ t: 'res', rid: m.rid }, out));
  });
  ws.on('close', () => detach(c));
  ws.on('error', () => { });
});
// garde les connexions ouvertes (hébergeurs qui coupent les connexions muettes)
setInterval(() => { for (const ws of wss.clients) { if (ws._zvDead) { ws.terminate(); continue; } ws._zvDead = true; ws.ping(); } }, 25000);
wss.on('connection', ws => ws.on('pong', () => { ws._zvDead = false; }));

(async () => {
  store = process.env.DATABASE_URL ? new PgStore(process.env.DATABASE_URL) : new FileStore(process.env.DATA_FILE || path.join(__dirname, 'data', 'users.json'));
  await store.load();
  server.listen(PORT, () => console.log(`Serveur Glasmort sur le port ${PORT} · ${users.size} comptes · stockage ${process.env.DATABASE_URL ? 'PostgreSQL' : 'fichier'}`));
})().catch(e => { console.error(e); process.exit(1); });
// sauvegarde finale à l'arrêt
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, async () => { try { await store.save([...users.keys()]); } catch (e) { } process.exit(0); });
