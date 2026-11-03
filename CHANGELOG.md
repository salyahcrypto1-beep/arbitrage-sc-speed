# Changes

## 2026-11-03 — portable local edition

- Exported application source from active server paths, including the running optimized arbitrage modules.
- Removed embedded Telegram/exchange/CMC secrets, personal administrator IDs and authenticated proxy pools. No production database or Git history imported.
- Added independent Docker Compose stacks, hidden-token setup, explicit first-owner pairing, public-only market-data adapters and diagnostics.
- Kept original calculation and Telegram modules; replaced server-specific ingestion in the standard launch path. Original exchange/WS sources remain for inspection.
- Added absolute book-age checks, conservative transfer metadata handling and contract mismatch filtering.
- Rejected order sizes exceeding available buy/sell depth and nonpositive transferable quantities.
- In the Market Maker edition, fixed ask/bid state assignment and corrected volume indexing in trade/snapshot comparison; replaced eval of order values with literal_eval.
- Added regression tests, entry-point import tests and Docker build CI.

Compatibility changes are intentional: no owner subscription database, no signed exchange wallet requests, no commercial proxy pool, laptop-sized default market coverage. Public-only network metadata does not cover every exchange.
