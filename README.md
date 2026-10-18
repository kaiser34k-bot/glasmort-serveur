# Serveur des comptes — Glasmort

Comptes (pseudo + mot de passe), progression, amis et groupes de 4 du jeu.
Node.js ≥ 18, une seule dépendance (`ws`) + `pg` pour PostgreSQL.

- `npm install && npm start` (port `PORT`, 8790 par défaut)
- `DATABASE_URL` défini → stockage PostgreSQL (ex. Neon) ; sinon fichier `data/users.json`
- `GET /health` → `ok` ; `GET /` → nombre de comptes / connectés / groupes
- `node test-protocol.js` (serveur lancé, `ZV_URL=ws://…`) : 26 vérifications du protocole
