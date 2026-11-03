# Instructions for local coding assistants

Read README.md, docs/DATA_COVERAGE.md and VALIDATION.md before setup.

- Standard launch: Python 3.12+, `python setup.py`, then `docker compose up -d --build`.
- Each repository is independent. Do not require the other repository, the original server, exchange credentials, or original subscriber data.
- Let the human type the BotFather token into setup.py's hidden input. Never print .env, tokens, auth URLs containing tokens, or raw credential-bearing error traces.
- Native WebSocket processes are launched by portable.native_ws. portable.feeds handles metadata only. Do not add CCXT or run duplicate feed processes.
- Public API data may be missing. Unknown deposit/withdraw status or fee must remain unknown and must not unlock a transfer route.
- Do not remove depth guards, freshness guards, contract mismatch guards, request timeouts, or retry backoff to make more signals appear.
- Never use the owner's original bot token or original administrator IDs. Local owner pairing is handled by portable/access.py.
- Preserve .env and MongoDB volumes when updating. Do not use down -v as a troubleshooting shortcut.
- No public database/cache ports are needed. Keep pickle snapshots inside the private Docker network.
- `python -m pytest -q` verifies computation and imports; it is not a live exchange or Telegram end-to-end test.
- Use `docker compose exec bot python -m portable.doctor --network` for local connectivity. Do not claim all exchanges are reachable merely because the code supports them.
- If changing behavior, add a targeted regression test and update CHANGELOG.md and architecture diagrams. Do not claim measured throughput or guaranteed profits without evidence.
