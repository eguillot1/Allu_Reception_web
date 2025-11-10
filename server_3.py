import os
import csv  # retained for backward compatibility (will phase out)
import base64  # legacy, used only if fallback code paths hit
import sqlite3  # legacy direct usage (now moved to storage module but kept for safety)
import threading
import json
from datetime import datetime
from typing import List, Optional
import time

import requests
import uvicorn
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Service layer imports (extracted modules)
from app.services.quartzy_service import (
    quartzy_service,
    safe_int,
    fetch_order_requests,
    quartzy_list_labs,
    quartzy_list_types,
    quartzy_inventory_search,
    quartzy_inventory_lookup,
    quartzy_collect_lab_locations,
    quartzy_find_inventory_match,
    quartzy_update_order_status,
    quartzy_decide_status,
    quartzy_update_inventory,
    _quartzy_debug,
)
from app.services.clickup_service import (
    send_to_clickup as ck_send_to_clickup,
    extract_task_meta as ck_extract_task_meta,
    upload_attachments_to_task as ck_upload_attachments_to_task,
    form_to_cf_ids as ck_form_to_cf_ids,
    STORAGE_DROPDOWN_IDS as CK_STORAGE_DROPDOWN_IDS,
    FORCED_STATUS_VALUE as CK_FORCED_STATUS_VALUE,
)
from app.services.power_automate_service import (
    post_to_power_automate as pa_post_to_power_automate,
    post_to_power_automate_structured as pa_post_structured,
)
from app.storage.logging_store import (
    ensure_db as storage_ensure_db,
    ensure_csv_with_header as storage_ensure_csv_with_header,
    log_reception_to_db as storage_log_reception_to_db,
    HEADERS as STORAGE_HEADERS,
)

# Backward compatibility (alias original names used throughout file)
# Always define local placeholders, then merge imported values.
form_to_cf_ids = globals().get('form_to_cf_ids', {})
STORAGE_DROPDOWN_IDS = globals().get('STORAGE_DROPDOWN_IDS', {})
FORCED_STATUS_VALUE = globals().get('FORCED_STATUS_VALUE', "")

form_to_cf_ids.update(ck_form_to_cf_ids)
FORCED_STATUS_VALUE = CK_FORCED_STATUS_VALUE or FORCED_STATUS_VALUE
STORAGE_DROPDOWN_IDS.update(CK_STORAGE_DROPDOWN_IDS)

# --------------------- Config ---------------------
load_dotenv()
CLICKUP_API_TOKEN = os.getenv("CLICKUP_API_TOKEN")
TEAM_ID = os.getenv("CLICKUP_TEAM_ID")
LIST_ID = os.getenv("CLICKUP_LIST_ID")
GROUP_ID = os.getenv("CLICKUP_RECEPTION_GROUP_ID")
POWER_AUTOMATE_WEBHOOK_URL = os.getenv("POWER_AUTOMATE_WEBHOOK_URL")

# Quartzy integration environment variables (official API)
# Supports old variable names for backward compatibility.
QUARTZY_API_TOKEN = os.getenv("QUARTZY_API_TOKEN")
QUARTZY_BASE_URL = os.getenv("QUARTZY_BASE_URL", "https://api.quartzy.com")
QUARTZY_ORG_ID = os.getenv("QUARTZY_ORG_ID")
QUARTZY_LAB_ID = os.getenv("QUARTZY_LAB_ID", "")  # optional filter
QUARTZY_ENABLED = os.getenv("QUARTZY_ENABLED", "1") not in ("0", "false", "False")
# Quartzy auth mode: access_token (Access-Token header), bearer (Authorization: Bearer), or auto (try both)
QUARTZY_AUTH_MODE = os.getenv("QUARTZY_AUTH_MODE", "auto").strip().lower()
QUARTZY_AUTH_FALLBACK = os.getenv("QUARTZY_AUTH_FALLBACK", "1") not in ("0", "false", "False")
QUARTZY_ORDERS_PAGE_LIMIT = int(os.getenv("QUARTZY_ORDERS_PAGE_LIMIT", "1") or 1)  # how many pages to fetch when all=1
QUARTZY_INVENTORY_SCAN_PAGES = int(os.getenv("QUARTZY_INVENTORY_SCAN_PAGES", "2") or 2)
QUARTZY_ENABLE_PARTIAL_STATUS = os.getenv("QUARTZY_ENABLE_PARTIAL_STATUS", "1") not in ("0", "false", "False")
QUARTZY_INVENTORY_SCAN_ENABLED = os.getenv("QUARTZY_INVENTORY_SCAN_ENABLED", "1") not in ("0", "false", "False")
QUARTZY_CACHE_TTL_S = int(os.getenv("QUARTZY_CACHE_TTL_S", "120") or 120)
QUARTZY_INVENTORY_LOOKUP_MAX_PAGES = int(os.getenv("QUARTZY_INVENTORY_LOOKUP_MAX_PAGES", "8") or 8)
APP_PREWARM_ENABLED = os.getenv("APP_PREWARM_ENABLED", "1") not in ("0", "false", "False")

def _normalize_quartzy_base(u: str) -> str:
    if not u:
        return "https://api.quartzy.com"
    u = u.strip()
    # Remove trailing slashes
    while u.endswith('/'):
        u = u[:-1]
    # If user supplied versioned base like .../v2 trim it (spec shows unversioned server)
    if u.lower().endswith('/v2'):
        u = u[:-3]
    return u
QUARTZY_BASE_URL = _normalize_quartzy_base(QUARTZY_BASE_URL)

# Runtime debug store for Quartzy operations
_quartzy_debug = {
    "last_orders_status": None,
    "last_orders_http": None,
    "last_orders_error": None,
    "last_orders_count": 0,
    "last_order_detail_id": None,
    "last_order_detail_http": None,
    "last_order_detail_error": None,
    "selected_orders_endpoint": None,
    "orders_attempts": [],
    "last_inventory_fetch": None,
    "last_inventory_update": None,
    "auth_mode_used": None,
    "orders_pages_fetched": 0,
    "inventory_scan_pages": 0,
    "last_auth_check": None,
}

# Simple in-process caches (not persistent) for Quartzy resources
_quartzy_caches = {
    "orders": {"data": None, "ts": 0},
    "labs": {"data": None, "ts": 0},
    "types": {"data": None, "ts": 0},
    "inventory_search": {},  # keyed by query lower -> {data, ts}
}

def _cache_get(key, subkey=None):
    now = time.time()
    c = _quartzy_caches.get(key)
    if not c:
        return None
    if key == "inventory_search" and subkey is not None:
        entry = c.get(subkey)
        if not entry:
            return None
        if now - entry.get("ts", 0) > QUARTZY_CACHE_TTL_S:
            return None
        return entry.get("data")
    else:
        if now - c.get("ts", 0) > QUARTZY_CACHE_TTL_S:
            return None
        return c.get("data")

def _cache_set(key, data, subkey=None):
    now = time.time()
    if key == "inventory_search" and subkey is not None:
        _quartzy_caches.setdefault("inventory_search", {})[subkey] = {"data": data, "ts": now}
    else:
        _quartzy_caches[key] = {"data": data, "ts": now}

raw_filter_values = os.getenv("CLICKUP_STATUS_FILTER_VALUES", "")
CLICKUP_STATUS_FILTER_VALUES = [v.strip() for v in raw_filter_values.split(",") if v.strip()]

STORAGE_DROPDOWN_IDS = {
    "-80°C": "d2a04fdb-cf6e-4cda-8d21-775c0c42e193",
    "-20°C": "53fdbee7-ef48-4aaa-91b2-a910efc89419",
    "4°C": "b354d914-5cda-4004-a088-13a553992c93",
    "Room Temperature": "b33860e2-d7c4-4169-af55-bb34b018ca8e",
}

# Storage to locations mapping (from Kivy app)
STORAGE_LOCATIONS = {
    "-80°C": ["Main -80°C", "-80°C Garage"],
    "-20°C": ["-20°C Left", "-20°C Right"],
    "4°C": ["Glass Door 4°C", "Lab Mini 4°C", "Machine Mini 4°C", "Storage Room 4°C", "BSL2 Mini 4°C"],
    "Room Temperature": [
        "Chemical Shelf", "Solvent Cabinet", "Acid Cabinet", "Base Cabinet",
        "Storage Room", "Wet Lab", "Other"
    ],
}
form_to_cf_ids = {
    # Explicitly constrained to the required mapping specified by user
    "comments": "4e732f3f-04dc-4f7f-9ffa-07317f769ba8",
    "location": "5373d5de-7ea2-4374-9c23-aff658c0e0f0",
    "received_by_id": "939760e2-1945-4570-b244-264e66bf9004",  # extracted numeric id from received_by form value
    "status": "8a0f12f6-9827-4d57-a2f6-0fe65160b3be",            # forced to option id below
    "storage_location": "05d2d9ce-9a51-45d9-9af7-1d4218d229de",  # value taken from location form key
    "sub_location": "e24983e9-71f7-44e5-b0fd-aabd391aa004",      # sub-location form key (hyphen) or fallback underscore
    "item_number": "e34a130b-733b-477f-a8de-3e8765831c44",       # number of samples (quantity)
    "clickup_received_date": "b4eca93f-a22a-47de-8937-267332673412",  # timestamp epoch ms
}
# Business rule overrides / derived mapping:
#  - Force status to a specific option value representing "Received" unless overridden by env
#  - Map item_number custom field from submitted quantity (number of samples)
#  - Received date custom field is the submission timestamp converted to epoch ms
#  - received_by_id already provided by frontend (selected user id)
#  - Comments stays as-is (also mirrored into task description)
#  - Storage/location/sub-location map 1:1 by name (if ambiguity existed, we assume storage_location maps to storage_location not regular location)
FORCED_STATUS_VALUE = (os.getenv("CLICKUP_RECEIVED_STATUS_VALUE", "11e8e2f0-c21a-4459-879d-e47fc09a36db") or "").strip()
MAP_ITEM_NUMBER_FROM_QUANTITY = os.getenv("CLICKUP_MAP_ITEM_NUMBER_FROM_QUANTITY", "1") not in ("0", "false", "False")
ENABLE_PARALLEL_UPDATES = os.getenv("CLICKUP_PARALLEL_UPDATES", "1") not in ("0", "false", "False")
PARALLEL_MAX_WORKERS = max(1, int(os.getenv("CLICKUP_PARALLEL_MAX_WORKERS", "5") or 5))
PROJECT_MANAGER_CF_ID = os.getenv("CLICKUP_CF_PROJECT_MANAGER_ID") or ""
BSL2_CF_ID = os.getenv("CLICKUP_CF_BSL2_STATUS_ID") or ""
# Dedicated BSL2 checkbox custom field ID (overrides generic BSL2_CF_ID usage for UI flag); can be overridden by env
BSL2_CHECKBOX_CF_ID = os.getenv("CLICKUP_CF_BSL2_CHECKBOX_ID", "f394f3db-0812-4ecf-91d8-2ea9a608762e")
cf_ids = os.getenv("CLICKUP_CUSTOM_FIELDS_IDS")
if not cf_ids:
    keep_cf_ids = list(form_to_cf_ids.values())
else:
    try:
        keep_cf_ids = [i.strip() for i in cf_ids.split(",") if i.strip()]
    except Exception:
        keep_cf_ids = list(form_to_cf_ids.values())

# Mapping specification (authoritative): each tuple: (field_name, clickup_field_id, source_form_key or derivation)
MAPPING_SPEC = [
    ("comments", form_to_cf_ids["comments"], "comments"),
    ("location", form_to_cf_ids["location"], "location"),
    ("received_by_id", form_to_cf_ids["received_by_id"], "extracted numeric from received_by / received_by_id"),
    ("status", form_to_cf_ids["status"], f"forced option id {FORCED_STATUS_VALUE or '11e8e2f0-c21a-4459-879d-e47fc09a36db'}"),
    ("storage_location", form_to_cf_ids["storage_location"], "storage_location label (mapped to ID)"),
    ("sub_location", form_to_cf_ids["sub_location"], "sub-location or sub_location"),
    ("item_number", form_to_cf_ids["item_number"], "quantity (# samples received)"),
    ("clickup_received_date", form_to_cf_ids["clickup_received_date"], "timestamp (epoch ms)")
]

def _validate_mapping():
    required = {"comments","location","received_by_id","status","storage_location","sub_location","item_number","clickup_received_date"}
    missing = [k for k in required if k not in form_to_cf_ids]
    if missing:
        print(f"[Mapping][WARN] Missing required custom field ids: {missing}")
_validate_mapping()

# --------------------- ClickUp Tasks Cache ---------------------
clickup_cache = {"data": None, "timestamp": 0}
clickup_cache_lock = threading.Lock()
CACHE_TTL = 180  # seconds (3 minutes)

# --------------------- App ---------------------
app = FastAPI(title="Allumiqs APP (Web)")

# Track router mount diagnostics
_ROUTER_MOUNT_LOG = []
def _attempt_mount(label: str, fn):  # pragma: no cover diagnostic helper
    try:
        fn()
        _ROUTER_MOUNT_LOG.append({"router": label, "status": "mounted"})
    except Exception as e:
        _ROUTER_MOUNT_LOG.append({"router": label, "status": "error", "error": str(e)})

def _mount_all():  # pragma: no cover
    # Delay imports until function call to surface errors clearly
    def _m_quartzy():
        from app.api.quartzy import router as _r; app.include_router(_r)
    def _m_clickup():
        from app.api.clickup import router as _r; app.include_router(_r)
    def _m_submission():
        from app.api.submission import router as _r; app.include_router(_r)
    _attempt_mount("quartzy", _m_quartzy)
    _attempt_mount("clickup", _m_clickup)
    _attempt_mount("submission", _m_submission)

# Initial automatic attempt
_mount_all()

# Prewarm caches on startup without blocking server start (can be disabled)
@app.on_event("startup")
async def _prewarm_caches():  # pragma: no cover
    if not APP_PREWARM_ENABLED:
        return
    try:
        import asyncio
        loop = asyncio.get_running_loop()
        def _warm():
            # Hard guard to keep prewarm modest in duration
            start = time.time()
            def _within_budget():
                return (time.time() - start) < 12.0  # 12s budget
            try:
                if _within_budget():
                    # Quartzy: warm ORDERED list and lab locations
                    quartzy_service.fetch_order_requests(statuses=["ORDERED"], all_pages=False, page_limit=1)
            except Exception:
                pass
            try:
                if _within_budget():
                    quartzy_service.collect_lab_locations_fast(lab_id=None, per_page=None, refresh=True)
            except Exception:
                pass
            try:
                if _within_budget():
                    # ClickUp: warm employees and sample tasks cache
                    from app.services.clickup_service import get_employees, get_samples_tasks
                    get_employees()
                    get_samples_tasks()
            except Exception:
                pass
        try:
            loop.run_in_executor(None, _warm)
        except Exception:
            pass
    except Exception:
        pass

# Diagnostic helper to list routes when using legacy entrypoint
@app.get("/_routes")
def _list_routes():  # pragma: no cover
    paths = []
    for r in app.routes:
        p = getattr(r, 'path', None) or getattr(r, 'path_format', None)
        if p:
            paths.append(p)
    return {"routes": sorted(set(paths)), "mount_log": _ROUTER_MOUNT_LOG}

@app.post("/_remount_routers")
def _remount():  # pragma: no cover
    _ROUTER_MOUNT_LOG.clear()
    _mount_all()
    return {"remounted": True, "log": _ROUTER_MOUNT_LOG}
# Resolve static directory robustly regardless of current working directory
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_STATIC_DIR = os.path.join(_BASE_DIR, "static")
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

@app.get("/")
def read_index():
    # Serve index.html using an absolute path to avoid CWD-related issues
    idx = os.path.join(_STATIC_DIR, "index.html")
    if os.path.exists(idx):
        return FileResponse(idx)
    # Fallback tiny page so users see something even if the file is missing
    return HTMLResponse("""<!doctype html><meta charset=\"utf-8\"><title>Allumiqs APP</title><body style=\"font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;padding:24px;color:#00334f;background:#f5f7fa;\"><h1>Allumiqs Reception</h1><p>Index file not found. Expected at: <code>/static/index.html</code>.</p></body>""")

@app.get("/healthz")
def _healthz():  # pragma: no cover
    return {"ok": True, "ts": int(time.time()), "prewarm_enabled": APP_PREWARM_ENABLED}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Compress JSON responses to speed up transfers (especially orders lists)
app.add_middleware(GZipMiddleware, minimum_size=500)

# Add caching headers for static assets; configurable via env var STATIC_CACHE_SECONDS
_STATIC_CACHE_SECONDS = int(os.getenv("STATIC_CACHE_SECONDS", "86400") or 86400)
@app.middleware("http")
async def _static_cache_headers(request, call_next):
    resp = await call_next(request)
    try:
        if request.url.path.startswith("/static/") and "Cache-Control" not in resp.headers:
            if _STATIC_CACHE_SECONDS and _STATIC_CACHE_SECONDS > 0:
                cc = f"public, max-age={_STATIC_CACHE_SECONDS}"
                if _STATIC_CACHE_SECONDS >= 3600:
                    cc += ", immutable"
                resp.headers["Cache-Control"] = cc
    except Exception:
        pass
    return resp

"""NOTE: Quartzy helper functions moved to app.services.quartzy_service.
Legacy wrappers can be restored if needed. Importing directly for Phase 2 extraction."""
from app.services.quartzy_service import (
    quartzy_headers,
    safe_int,
    quartzy_fetch_order_requests,
    quartzy_list_labs,
    quartzy_list_types,
    quartzy_inventory_search,
    quartzy_inventory_lookup,
    quartzy_collect_lab_locations,
    quartzy_find_inventory_match,
    quartzy_update_order_status,
    quartzy_decide_status,
    quartzy_update_inventory,
    _quartzy_debug,
)

"""NOTE (Refactor): The following endpoint implementations have been migrated to
APIRouters under app/api/*. This file now retains only legacy helper shims.
Removed routes:
  /api/upload
  /api/samples_tasks, /api/samples_tasks/{task_id}
  /api/employees
  /api/storage_locations
  /api/quartzy/*
  /api/submit

If this file receives further direct HTTP traffic at those paths it will now be
handled by the router versions mounted in app.main. This block intentionally
left in place for historical context.
"""

# --------------------- Helpers ---------------------
# Storage constants now sourced from storage module
HEADERS = STORAGE_HEADERS
CSV_PATH = "reception_log.csv"  # retained for backward compatibility in existing file paths
DB_PATH = "reception_log.db"

# Backwards-compatible shim functions to avoid changing callsites
def ensure_db():
    storage_ensure_db()

def log_reception_to_db(row):
    storage_log_reception_to_db(row)

def ensure_csv_with_header():
    storage_ensure_csv_with_header()

# --------------------- Power Automate Helper ---------------------
def post_to_power_automate_structured(**kwargs):  # shim to new service
    return pa_post_structured(**kwargs)

def post_to_power_automate(data: dict):  # shim wrapper
    success, msg, dbg = pa_post_to_power_automate(data)
    if not success:
        print(f"[Power Automate] Failed: {msg}")
    return success, msg, dbg

## (Removed ClickUp task endpoints now provided by router.)
def send_to_clickup(data):  # shim retained for legacy internal calls
    from app.services.clickup_service import send_to_clickup as _send
    return _send(data)

# --------------------- Main ---------------------
if __name__ == "__main__":
    uvicorn.run("server_3:app", host="0.0.0.0", port=8000, reload=True)
