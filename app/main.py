"""Transitional application entrypoint.

Uses the existing FastAPI app defined in `server_3.py` so you can
run either:
  uvicorn app.main:app --reload
OR the legacy:
  python server_3.py

Subsequent phases will replace inline endpoints with APIRouters here.
"""
from fastapi import FastAPI
from server_3 import app as legacy_app  # existing monolith app (already created in server_3)
# NOTE: Quartzy router intentionally excluded on main branch.
from app.api.clickup import router as clickup_router
from app.api.submission import router as submission_router

# Reuse the already-built legacy FastAPI instance and mount new routers onto it.
app: FastAPI = legacy_app
# Quartzy: NOT mounted on main. Present in 'quartzy-dev' only until stabilized.
app.include_router(clickup_router)
app.include_router(submission_router)

__all__ = ["app"]
