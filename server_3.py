"""
server_3.py — Entrypoint FastAPI pour l'app Allumiqs Reception.

Cette version a été rendue plus robuste avec :
- Logging structuré (module `logging`) au lieu de `print`.
- Protection optionnelle des endpoints internes de debug (`/_routes`, `/_remount_routers`) via env.
- Gestion de cache Quartzy thread-safe.
- CORS configurable via variables d'environnement.
- Commentaires et docstrings plus explicites.

Les modifications majeures sont marquées par des blocs `# CHANGE:` avec
RAISON + IMPACT pour faciliter la revue.
"""

import os
import csv  # LEGACY: conservé pour compatibilité éventuelle avec du code legacy.
import base64  # LEGACY: idem.
import sqlite3  # LEGACY: idem (usage direct remplacé par storage module).
import threading
import json  # Peut servir à la sérialisation dans d'autres modules.
from datetime import datetime
from typing import List, Optional
import time
import logging  # CHANGE: ajout de logging structuré (RAISON/IMPACT ci-dessous)

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from dotenv import load_dotenv

# Service layer imports (extracted modules)
from app.services.quartzy_service import (
    quartzy_service,
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

# ---------------------------------------------------------------------------
# CHANGE: configuration du logger
# RAISON : remplacer les `print` dispersés par un logging structuré,
#          intégrable avec Uvicorn / infra.
# IMPACT : meilleure observabilité, logs centralisés, niveaux (INFO/WARN/ERROR).
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

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
    """
    Normalise l'URL de base Quartzy :
    - supprime les slashs de fin
    - supprime /v2 si présent (l'API référence un serveur non versionné)
    """
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

# ---------------------------------------------------------------------------
# CHANGE: debug store Quartzy
# RAISON : auparavant, il y avait une redéfinition locale de _quartzy_debug
#          qui écrasait potentiellement celle du service. Ici, on documente
#          simplement que le debug détaillé est géré côté service.
# IMPACT : moins de confusion, une seule source de vérité pour le debug
#          détaillé Quartzy (dans app.services.quartzy_service).
# ---------------------------------------------------------------------------
# NOTE: Le debug détaillé est maintenant géré côté quartzy_service.
# Si besoin, on peut relayer certains éléments ici via des endpoints dédiés.

# --------------------- In-process Caches ---------------------
# Simple in-process caches (not persistent) for Quartzy resources.
# Avec verrou pour la sécurité en environnement multi-threads.
_quartzy_caches = {
    "orders": {"data": None, "ts": 0},
    "labs": {"data": None, "ts": 0},
    "types": {"data": None, "ts": 0},
    "inventory_search": {},  # keyed by query lower -> {data, ts}
}

# ---------------------------------------------------------------------------
# CHANGE: ajout d'un verrou pour les caches Quartzy
# RAISON : sécuriser les accès en lecture/écriture si FastAPI tourne
#          avec plusieurs threads (accès concurrent aux dicts).
# IMPACT : évite des race conditions potentielles sur les caches,
#          sans modifier l'API publique ni la logique métier.
# ---------------------------------------------------------------------------
_quartzy_cache_lock = threading.Lock()

def _cache_get(key, subkey=None):
    """
    Récupère une entrée du cache Quartzy si non expirée.
    """
    now = time.time()
    with _quartzy_cache_lock:
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
    """
    Met à jour le cache Quartzy avec un timestamp.
    """
    now = time.time()
    with _quartzy_cache_lock:
        if key == "inventory_search" and subkey is not None:
            _quartzy_caches.setdefault("inventory_search", {})[subkey] = {"data": data, "ts": now}
        else:
            _quartzy_caches[key] = {"data": data, "ts": now}

raw_filter_values = os.getenv("CLICKUP_STATUS_FILTER_VALUES", "")
CLICKUP_STATUS_FILTER_VALUES = [v.strip() for v in raw_filter_values.split(",") if v.strip()]

# Storage dropdown mapping (ClickUp custom field options)
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

# Mapping formulaire -> IDs de champs custom ClickUp
form_to_cf_ids = {
    # Explicitement contraint à la mapping requise par l'usage métier
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
        # CHANGE: logging au lieu de swallow silencieux
        # RAISON : détecter rapidement un mauvais format de env.
        # IMPACT : un warning apparaît dans les logs, mais on reste
        #          fonctionnel avec la mapping par défaut.
        logger.warning("Invalid CLICKUP_CUSTOM_FIELDS_IDS env value; falling back to default mapping.")
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
    """
    Vérifie que tous les champs custom nécessaires sont bien présents
    dans form_to_cf_ids. Loggue un warning si certains manquent.
    """
    required = {
        "comments",
        "location",
        "received_by_id",
        "status",
        "storage_location",
        "sub_location",
        "item_number",
        "clickup_received_date",
    }
    missing = [k for k in required if k not in form_to_cf_ids]
    if missing:
        # CHANGE: logging structuré
        # RAISON : diagnostiquer facilement une mauvaise config ClickUp.
        # IMPACT : aucun changement de comportement, mais warning explicite.
        logger.warning("[Mapping] Missing required custom field ids: %s", missing)

_validate_mapping()

# --------------------- ClickUp Tasks Cache ---------------------
# NOTE: cache global ClickUp, aujourd'hui peu utilisé dans ce fichier,
# mais conservé pour compatibilité / usage futur.
clickup_cache = {"data": None, "timestamp": 0}
clickup_cache_lock = threading.Lock()
CACHE_TTL = 180  # seconds (3 minutes)

# --------------------- App ---------------------
app = FastAPI(title="Allumiqs APP (Web)")

# Track router mount diagnostics
_ROUTER_MOUNT_LOG = []

def _attempt_mount(label: str, fn):  # pragma: no cover diagnostic helper
    """
    Tente de monter un router et loggue le résultat dans _ROUTER_MOUNT_LOG
    pour diagnostic.
    """
    try:
        fn()
        _ROUTER_MOUNT_LOG.append({"router": label, "status": "mounted"})
        logger.info("Router '%s' mounted successfully.", label)
    except Exception as e:
        _ROUTER_MOUNT_LOG.append({"router": label, "status": "error", "error": str(e)})
        logger.error("Failed to mount router '%s': %s", label, e)

def _mount_all():  # pragma: no cover
    """
    Monte tous les routers d'API (quartzy, clickup, submission).
    Import différé pour que les erreurs soient visibles clairement.
    """
    def _m_quartzy():
        from app.api.quartzy import router as _r
        app.include_router(_r)

    def _m_clickup():
        from app.api.clickup import router as _r
        app.include_router(_r)

    def _m_submission():
        from app.api.submission import router as _r
        app.include_router(_r)

    _attempt_mount("quartzy", _m_quartzy)
    _attempt_mount("clickup", _m_clickup)
    _attempt_mount("submission", _m_submission)

# Initial automatic attempt
_mount_all()

# --------------------- Prewarm Caches on Startup ---------------------
@app.on_event("startup")
async def _prewarm_caches():  # pragma: no cover
    """
    Pré-chauffe certains caches (Quartzy, ClickUp) au démarrage sans
    bloquer le serveur. Protégé par APP_PREWARM_ENABLED.
    """
    if not APP_PREWARM_ENABLED:
        logger.info("APP_PREWARM_ENABLED=0: skipping cache prewarm.")
        return
    try:
        import asyncio
        loop = asyncio.get_running_loop()

        def _warm():
            # Hard guard to keep prewarm modest in duration
            start = time.time()

            def _within_budget():
                return (time.time() - start) < 12.0  # 12s budget

            # CHANGE: ajout de logging autour des erreurs
            # RAISON : auparavant, toutes les exceptions étaient silencieuses.
            # IMPACT : meilleure observabilité si Quartzy ou ClickUp sont cassés,
            #          sans empêcher le serveur de démarrer.
            try:
                if _within_budget():
                    # Quartzy: warm ORDERED list
                    logger.debug("Prewarm: fetching Quartzy ORDERED requests (page_limit=1).")
                    quartzy_service.fetch_order_requests(statuses=["ORDERED"], all_pages=False, page_limit=1)
            except Exception as e:
                logger.warning("Prewarm Quartzy orders failed: %s", e)

            try:
                if _within_budget():
                    logger.debug("Prewarm: collecting Quartzy lab locations.")
                    quartzy_service.collect_lab_locations_fast(lab_id=None, per_page=None, refresh=True)
            except Exception as e:
                logger.warning("Prewarm Quartzy lab locations failed: %s", e)

            try:
                if _within_budget():
                    # ClickUp: warm employees and sample tasks cache
                    from app.services.clickup_service import get_employees, get_samples_tasks
                    logger.debug("Prewarm: fetching ClickUp employees.")
                    get_employees()
                    logger.debug("Prewarm: fetching ClickUp sample tasks.")
                    get_samples_tasks()
            except Exception as e:
                logger.warning("Prewarm ClickUp cache failed: %s", e)

        try:
            loop.run_in_executor(None, _warm)
        except Exception as e:
            logger.error("Failed to schedule prewarm in executor: %s", e)
    except Exception as e:
        logger.error("Prewarm startup hook failed: %s", e)

# --------------------- Diagnostic Routes ---------------------

# ---------------------------------------------------------------------------
# CHANGE: protection des endpoints de debug
# RAISON : éviter d'exposer /_routes et /_remount_routers en production
#          sans contrôle, pour limiter la surface d'attaque/information.
# IMPACT : par défaut (DEBUG_ROUTES_ENABLED=0), ces routes renvoient 404.
#          Pour les réactiver (ex. en dev), définir DEBUG_ROUTES_ENABLED=1.
# ---------------------------------------------------------------------------
DEBUG_ROUTES_ENABLED = os.getenv("DEBUG_ROUTES_ENABLED", "0") in ("1", "true", "True")

@app.get("/_routes")
def _list_routes():  # pragma: no cover
    """
    Endpoint de diagnostic qui liste toutes les routes montées
    et le log de montage. Désactivé en prod via DEBUG_ROUTES_ENABLED.
    """
    if not DEBUG_ROUTES_ENABLED:
        return JSONResponse(status_code=404, content={"detail": "Not found"})
    paths = []
    for r in app.routes:
        p = getattr(r, 'path', None) or getattr(r, 'path_format', None)
        if p:
            paths.append(p)
    return {"routes": sorted(set(paths)), "mount_log": _ROUTER_MOUNT_LOG}

@app.post("/_remount_routers")
def _remount():  # pragma: no cover
    """
    Endpoint de diagnostic permettant de remonter dynamiquement
    les routers. Désactivé en prod via DEBUG_ROUTES_ENABLED.
    """
    if not DEBUG_ROUTES_ENABLED:
        return JSONResponse(status_code=404, content={"detail": "Not found"})
    _ROUTER_MOUNT_LOG.clear()
    _mount_all()
    return {"remounted": True, "log": _ROUTER_MOUNT_LOG}

# --------------------- Static Files & Root ---------------------

# Resolve static directory robustly regardless of current working directory
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_STATIC_DIR = os.path.join(_BASE_DIR, "static")
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

@app.get("/")
def read_index():
    """
    Sert le fichier index.html de l'app front (SPA).
    Si le fichier n'existe pas, renvoie une petite page HTML de fallback.
    """
    # Serve index.html using an absolute path to avoid CWD-related issues
    idx = os.path.join(_STATIC_DIR, "index.html")
    if os.path.exists(idx):
        return FileResponse(idx)
    # Fallback tiny page so users see something even if the file is missing
    return HTMLResponse(
        """<!doctype html><meta charset="utf-8">
<title>Allumiqs APP</title>
<body style="font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
             padding:24px;color:#00334f;background:#f5f7fa;">
<h1>Allumiqs Reception</h1>
<p>Index file not found. Expected at: <code>/static/index.html</code>.</p>
</body>"""
    )

@app.get("/healthz")
def _healthz():  # pragma: no cover
    """
    Endpoint de healthcheck simple pour monitoring / orchestrateurs.
    """
    return {"ok": True, "ts": int(time.time()), "prewarm_enabled": APP_PREWARM_ENABLED}

# --------------------- Middleware Configuration ---------------------

# ---------------------------------------------------------------------------
# CHANGE: CORS configurable via env
# RAISON : auparavant, allow_origins=["*"] en dur. On expose toujours "*"
#          par défaut pour compatibilité, mais on permet de restreindre via
#          CORS_ALLOWED_ORIGINS.
# IMPACT : si CORS_ALLOWED_ORIGINS n'est pas défini, comportement identique.
#          Sinon, la liste d'origines est utilisée.
# ---------------------------------------------------------------------------
_raw_origins = os.getenv("CORS_ALLOWED_ORIGINS", "*")
if _raw_origins.strip() == "*":
    CORS_ALLOWED_ORIGINS = ["*"]
else:
    CORS_ALLOWED_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]

CORS_ALLOW_CREDENTIALS = os.getenv("CORS_ALLOW_CREDENTIALS", "1") not in ("0", "false", "False")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOWED_ORIGINS,
    allow_credentials=CORS_ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Compress JSON responses to speed up transfers (especially orders lists)
app.add_middleware(GZipMiddleware, minimum_size=500)

# Add caching headers for static assets; configurable via env var STATIC_CACHE_SECONDS
_STATIC_CACHE_SECONDS = int(os.getenv("STATIC_CACHE_SECONDS", "86400") or 86400)

@app.middleware("http")
async def _static_cache_headers(request, call_next):
    """
    Ajoute des headers Cache-Control aux ressources statiques si non présents.
    """
    resp = await call_next(request)
    try:
        if request.url.path.startswith("/static/") and "Cache-Control" not in resp.headers:
            if _STATIC_CACHE_SECONDS and _STATIC_CACHE_SECONDS > 0:
                cc = f"public, max-age={_STATIC_CACHE_SECONDS}"
                if _STATIC_CACHE_SECONDS >= 3600:
                    cc += ", immutable"
                resp.headers["Cache-Control"] = cc
    except Exception as e:
        # CHANGE: logging de l'erreur au lieu de swallow silencieux
        # RAISON : diagnostiquer des problèmes de middleware si besoin.
        # IMPACT : aucun effet sur la réponse, mais trace dans les logs.
        logger.debug("Failed to set static cache headers: %s", e)
    return resp

# --------------------- Legacy Helpers & Shims ---------------------
# NOTE: Quartzy helper functions moved to app.services.quartzy_service.
# Legacy wrappers peuvent être réintroduits si nécessaire. Ce fichier
# conserve uniquement les shims nécessaires à la compatibilité historique.

# Storage constants now sourced from storage module
HEADERS = STORAGE_HEADERS
CSV_PATH = "reception_log.csv"  # retained for backward compatibility in existing file paths
DB_PATH = "reception_log.db"

# Backwards-compatible shim functions to avoid changing callsites
def ensure_db():
    """
    Shim vers app.storage.logging_store.ensure_db().
    """
    return storage_ensure_db()

def log_reception_to_db(row):
    """
    Shim vers app.storage.logging_store.log_reception_to_db(row).
    """
    return storage_log_reception_to_db(row)

def ensure_csv_with_header():
    """
    Shim vers app.storage.logging_store.ensure_csv_with_header().
    """
    return storage_ensure_csv_with_header()

# --------------------- Power Automate Helper ---------------------
def post_to_power_automate_structured(**kwargs):
    """
    Shim vers la fonction structurée de Power Automate.
    """
    return pa_post_structured(**kwargs)

def post_to_power_automate(data: dict):
    """
    Shim wrapper vers Power Automate avec logging en cas d'échec.
    """
    success, msg, dbg = pa_post_to_power_automate(data)
    if not success:
        # CHANGE: logging structuré au lieu de print
        # RAISON : tracer les erreurs d'intégration Power Automate.
        # IMPACT : facilite le debugging et l'alerte.
        logger.error("[Power Automate] Failed: %s", msg)
    return success, msg, dbg

# --------------------- ClickUp Shim ---------------------
def send_to_clickup(data):
    """
    Shim conservé pour les appels legacy internes vers ClickUp.
    """
    return ck_send_to_clickup(data)

# --------------------- Main ---------------------
if __name__ == "__main__":
    # NOTE: le logging global peut être configuré ici si ce module est lancé
    # directement (ex: python server_3.py). Uvicorn configure normalement ses
    # propres loggers quand utilisé via `uvicorn server_3:app`.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    uvicorn.run("server_3:app", host="0.0.0.0", port=8000, reload=True)
