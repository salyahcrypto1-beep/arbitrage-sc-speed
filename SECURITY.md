# Secrets and local data

Only the owner's new Telegram bot token is required. setup.py keeps it in local .env with restrictive permissions where supported. .env and local data are excluded from Git and build context. Do not paste secrets into issues or AI chats.

The source export excludes production databases, subscriber records, logs, paid proxy addresses, hard-coded exchange credentials and historical Git revisions. Local pairing uses a random code, then a persistent owner record. Code pairing is not the Telegram token.

Docker services have no published host ports. The historical cache protocol includes pickle, so cache contents must be trusted: never expose MongoDB, Redis or Memcached to the internet. Sharing a repository does not share any running bot or its local database.

Report defects privately to the repository owner, without tokens or private user records. Third-party API schemas and limits can change independently of this repository.
