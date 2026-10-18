// Test du protocole : node test-protocol.js (le serveur doit tourner, données de test jetables)
const WebSocket = require('ws'), URL = process.env.ZV_URL || 'ws://localhost:8790';
function client(name) {
  const ws = new WebSocket(URL), C = { name, ws, rid: 0, wait: {}, last: {}, events: [] };
  ws.on('message', d => { const m = JSON.parse(d); if (m.t === 'res') { C.wait[m.rid](m); return; } C.last[m.t] = m; C.events.push(m); });
  C.req = (t, o = {}) => new Promise(r => { const rid = ++C.rid; C.wait[rid] = r; ws.send(JSON.stringify(Object.assign({ t, rid }, o))); });
  C.open = new Promise(r => ws.on('open', r)); return C;
}
const sleep = ms => new Promise(r => setTimeout(r, ms));
const ok = (c, m) => { if (!c) { console.error('ÉCHEC', m); process.exitCode = 1; } else console.log('ok', m); };
(async () => {
  const s = Date.now() % 100000, A = client('a'), B = client('b'), D = client('d');
  await Promise.all([A.open, B.open, D.open]);
  const na = 'Alice' + s, nb = 'Bob' + s, nd = 'Dan' + s;
  ok((await A.req('register', { name: na, pass: 'secret1' })).ok, 'inscription A');
  ok(!(await B.req('register', { name: na, pass: 'secret1' })).ok, 'pseudo déjà pris');
  ok((await B.req('register', { name: nb, pass: 'secret2' })).ok, 'inscription B');
  ok((await D.req('register', { name: nd, pass: 'secret3' })).ok, 'inscription D');
  ok(!(await A.req('login', { name: nb, pass: 'faux' })).ok, 'mauvais mot de passe refusé');
  const r = await A.req('progPut', { prog: { lv: 12, xp: 300 } }); ok(r.ok, 'progression envoyée');
  const A2 = client('a2'); await A2.open; const lg = await A2.req('login', { name: na.toLowerCase(), pass: 'secret1' });
  ok(lg.ok && lg.prog && lg.prog.lv === 12, 'connexion depuis un autre PC + progression récupérée');
  const rs = await client('a3'); await rs.open; ok((await rs.req('resume', { name: na, token: lg.token })).ok, 'reprise de session par jeton');
  ok((await A.req('friendAdd', { name: nb })).ok, 'demande d’ami A→B'); await sleep(100);
  ok(B.last.me && B.last.me.reqIn.includes(na), 'B voit la demande');
  ok((await B.req('friendAccept', { name: na })).ok, 'B accepte'); await sleep(100);
  ok(A.last.me.friends.some(f => f.name === nb && f.online), 'A voit B en ligne');
  ok(!(await A.req('partyInvite', { name: nd })).ok, 'invitation refusée si pas ami');
  await A.req('friendAdd', { name: nd }); await D.req('friendAccept', { name: na });
  await A.req('status', { hero: 'nael', map: 'montouliers' }); await B.req('status', { hero: 'pablo' }); await D.req('status', { hero: 'gabriel' });
  ok((await A.req('partyInvite', { name: nb })).ok, 'A invite B'); await sleep(100);
  ok(B.last.invite && B.last.invite.from === na, 'B reçoit l’invitation');
  ok((await B.req('partyAccept', { id: B.last.invite.id })).ok, 'B rejoint');
  await A.req('partyInvite', { name: nd }); await sleep(100); await D.req('partyAccept', { id: D.last.invite.id }); await sleep(100);
  const P = A.last.party.party; ok(P.members.length === 3 && P.members.find(m => m.name === nb).hero === 'pablo', 'groupe de 3 avec les personnages');
  ok(!(await B.req('partyMap', { map: 'train' })).ok, 'seul le chef choisit la carte');
  ok((await A.req('partyMap', { map: 'train' })).ok, 'le chef change de carte'); await sleep(100);
  ok(D.last.party.party.map === 'train', 'carte transmise');
  await B.req('partyChat', { text: 'salut !' }); await sleep(100); ok(D.last.party.party.chat.some(c => c.text === 'salut !'), 'discussion du groupe');
  ok((await A.req('partyLaunch', { code: 'ABCDE' })).ok, 'lancement'); await sleep(100);
  ok(B.last.launch && B.last.launch.code === 'ABCDE' && D.last.launch.map === 'train', 'code transmis sans le taper');
  B.ws.close(); await sleep(300); ok(A.last.party.party.members.find(m => m.name === nb).online === false, 'déconnexion : place gardée (rechargement)');
  ok((await D.req('partyLeave')).ok, 'D quitte'); await sleep(100); ok(A.last.party.party.members.length === 2, 'groupe à 2');
  for (const c of [A, A2, D, rs]) c.ws.close();
  setTimeout(() => process.exit(), 200);
})();
