# Delphi 5.0 Phase 6 Hosted Shell

## Now Usable In Hosted Delphi 5.0

- Bill can sign in through the hosted browser login entry flow at `/hosted/login` using Supabase-compatible email/password authentication.
- Authenticated private-access users can open a hosted Delphi 5.0 browser shell at `/hosted`.
- The hosted shell now includes browser pages for performance summary, journal trades, and open trades.
- The hosted shell now also includes real browser routes for Apollo, Kairos, and Manage Trades, with Manage Trades already reusing the hosted open-trades action contract.
- These hosted pages are backed by the hosted action/data layer, so browser rendering and future clients share the same protected payload contracts.

## Still Local-Only

- Apollo execution and deeper Apollo UI/workflow actions remain on the local runtime.
- Kairos execution, replay workflows, runner controls, and deeper Kairos UI remain on the local runtime.
- Trade entry, trade editing, imports, provider login flows, and active management actions remain local-only.
- The existing Delphi local routes and templates remain the active default runtime experience.

## Next Integration Target

- Add hosted Apollo and Kairos summary/action endpoints so the new hosted shell pages can move beyond placeholders.
- Introduce hosted read-only detail views for individual trades and management records.
- Decide when the hosted shell should start owning authenticated browser navigation more broadly.