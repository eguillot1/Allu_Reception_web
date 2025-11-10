# Refactor Plan (Phase 1 Scaffold)

This document describes the SAFE incremental extraction strategy for the existing monolithic `server_3.py`.

## Goals
- Preserve current runtime behavior EXACTLY.
- Introduce clearer layering to support future features (additional inventory fields, new vendors, more APIs).
- Allow partial rollbacks easily (keep `server_3.py` authoritative until a phase is marked complete).
- Enable unit testing of isolated services without starting the entire app.

## Principles
1. Behavior Freeze: ClickUp pathways are considered stable/frozen; changes isolated to service extraction only (no logic changes during move).
2. Single Source of Truth per Phase: During a phase, both old and new modules may exist, but only one is imported by runtime code.
3. Small Commits: Each extraction step keeps the app startable.
4. Parallel Coexistence: New services reference a shared `app.config.CONFIG` so env parsing stays centralized.

## Proposed Package Layout
```
app/
  config.py              # Central environment loading (already added)
  services/
    clickup_service.py   # (Phase 2) Pure functions for ClickUp interactions
    quartzy_service.py   # (Phase 2) Quartzy helpers (moved from monolith)
    power_automate_service.py  # (Phase 3) Structured PA posting logic
  api/
    __init__.py          # (Phase 4) APIRouter definitions (samples, quartzy, submit)
  storage/
    __init__.py          # (Phase 5) CSV + SQLite persistence layer
  models/
    __init__.py          # (Phase 6) Pydantic models for request/response schemas
server_3.py              # Transitional entrypoint (shrinks over phases)
```

## Phase Breakdown
- Phase 1 (DONE): Add `app.config` with immutable dataclasses. No usage change yet.
- Phase 2 (DONE): Extract Quartzy + ClickUp + Power Automate + storage helpers into `app/services` & `app/storage` with shims in monolith.
- Phase 3 (IN PROGRESS): Introduce alternate entrypoint `app/main.py` (added) that simply re-exports existing app.
- Phase 4 (NEXT): Create `api/` package with APIRouters (samples, quartzy, submit). Replace endpoint bodies in `server_3.py` with thin proxies logging deprecation.
- Phase 5: Consolidate environment/config usage so services read from `app.config.CONFIG`; remove duplicate env loads.
- Phase 6: Introduce Pydantic models (input form, submission response, debug objects). Replace bare dict returns gradually.
- Phase 7: Add tests for each service (mock HTTP). Introduce interface boundaries & contract tests.
- Phase 8: Deprecate `server_3.py` after a stability window; keep a stub pointing to `app.main:app`.

## Safety Mechanisms
- Feature flags (existing QUARTZY_ENABLED, etc.) remain honored.
- Each moved function retains identical signature and docstring. A deprecated wrapper can be left behind temporarily that calls the new location and logs a `[DEPRECATED_PROXY]` message.
- Error handling remains unchanged until after full extraction.

## Immediate Next Step
If approved: Implement Phase 2 by creating `quartzy_service.py` and `clickup_service.py` with direct copies of logic and update imports in `server_3.py`.

## Rollback Plan
Delete new module file(s), revert import lines to original inline definitions (git revert simple since phases are small). No schema migrations needed.

## Future Enhancements Enabled
- Async HTTP (httpx) swap isolated to service layer.
- Central retry/backoff policies.
- Structured logging & observability injection (correlation IDs).
- Unit tests without hitting live APIs.

---
This plan is intentionally conservative to **avoid breaking any current functionality** while paving the way for safe evolution.
