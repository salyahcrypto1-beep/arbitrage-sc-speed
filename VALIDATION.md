# Validation

This file distinguishes verified behavior from assumptions.

- Python source compilation and targeted pytest regression/import tests are run during preparation and in GitHub Actions.
- Tests cover fees, insufficient depth, stale equal-time books, transfer permissions, contract mismatch, and public symbol normalization. The MM repository also checks separate ask/bid state.
- Import checks load every production entry point with a dummy token and do not poll Telegram.
- CI builds the Linux Docker image. This does not by itself prove every public exchange API is reachable in your country or VPN configuration.
- Live unauthenticated REST probes from the preparation computer confirmed BTC order books from several exchanges; others had DNS, HTTP 503 or timeouts. These are point-in-time network observations, not guaranteed availability.
- A complete live Telegram run with a newly created user's token must be completed on that user's computer. Original production tokens are never used to start a duplicate bot.
- macOS/Windows native Python service deployment is not the supported installation path; Docker is the portable runtime. No GPU is required.

Reproduce unit checks: `pip install -r requirements.txt pytest pytest-asyncio`, then `python -m pytest -q` from the repository root.

Local acceptance: see docs/AI_SETUP.md. After startup, run `docker compose exec bot python -m portable.doctor --network` and verify your bot's response to /start.
