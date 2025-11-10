# Reception App

FastAPI-based reception logging and integrations (ClickUp, Power Automate). Quartzy-related functionality exists but will live only on a separate development branch (`quartzy-dev`) initially.

## Features (main branch)
- Submit "Samples" and "Other" reception forms
- ClickUp project/task metadata
- Power Automate webhook integration
- Static single-page frontend served by FastAPI

## Deferred (quartzy-dev branch)
- Quartzy inventory search, quantity update flows, auto-add UI automation
- Partial order reception adjustments

## Running Locally
```
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
Visit: http://localhost:8000/

## Environment Variables
See `.env.example` for all supported variables. Create a `.env` file and populate required secrets.

## Tests
```
pytest -q
```
(Quartzy-specific tests only exist on `quartzy-dev`.)

## Branch Strategy
| Branch | Purpose |
|--------|---------|
| main | Stable, deployable, Quartzy routes excluded |
| quartzy-dev | Full feature set including Quartzy (inventory/order) integration |
| feat/*, fix/*, chore/* | Short-lived branches off the relevant base (usually main; Quartzy changes off quartzy-dev) |

Merge flow:
1. Develop Quartzy changes on `quartzy-dev`.
2. When ready to promote Quartzy features, open PR from `quartzy-dev` -> `main`.
3. After merge/tag, rebase or merge `main` back into `quartzy-dev` if it continues to diverge.

## Adding Quartzy Back Later
Merge `quartzy-dev` into `main` once inventory/order features are production-ready. Ensure env vars present and run integration tests.

## License
(Choose a license and add a LICENSE file.)
