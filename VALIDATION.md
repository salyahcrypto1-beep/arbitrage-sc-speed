# Validation

This file distinguishes verified behavior from assumptions.

- Python source compilation and targeted pytest regression/import tests are run during preparation and in GitHub Actions.
- Tests cover fees, insufficient depth, stale equal-time books, transfer permissions, contract mismatch, and public symbol normalization. The MM repository also checks separate ask/bid state.
- Import checks load every calculation/client entry point and all ten native WebSocket modules with a dummy token and do not poll Telegram.
- CI builds the Linux Docker image. This does not by itself prove every public exchange API is reachable in your country or VPN configuration.
- Live unauthenticated REST probes from the preparation computer confirmed BTC order books from several exchanges; others had DNS, HTTP 503 or timeouts. These are point-in-time network observations, not guaranteed availability.
- A complete live Telegram run with a newly created user's token must be completed on that user's computer. Original production tokens are never used to start a duplicate bot.
- macOS/Windows native Python service deployment is not the supported installation path; Docker is the portable runtime. No GPU is required.

Reproduce unit checks: `pip install -r requirements.txt pytest pytest-asyncio`, then `python -m pytest -q` from the repository root.

Local acceptance: see docs/AI_SETUP.md. After startup, run `docker compose exec bot python -m portable.doctor --network` and verify your bot's response to /start.

The revised transport uses original native WebSocket classes. Unit/import checks do not prove every exchange live stream is available. No CCXT is installed. Local single-user access and removal of inherited administration have dedicated regression checks.

Native transport revision live check: each of the ten original exchange-specific modules received a BTC/USDT order book in an isolated public-network probe (35-second limit). CoinEx required updating the native subscription to v2. This is not an endurance, full-market or Telegram end-to-end test. Public REST ticker discovery succeeded for eight exchanges; CoinEx REST returned HTTP 503 and BitMart REST had a DNS error from this machine. Explicit SYMBOLS bypasses REST market discovery for subscriptions, not missing transfer metadata.
