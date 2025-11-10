"""Quartzy API Router.

Import the service module as a namespace to keep monkeypatching straightforward.
"""
from __future__ import annotations
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Query, Body
from fastapi.responses import FileResponse, JSONResponse
import os

# Import wrapper functions from service so tests can monkeypatch them
import app.services.quartzy_service as quartzy_mod

router = APIRouter(prefix="/api/quartzy", tags=["quartzy"])
PENDING_INV_CSV = "quartzy_inventory_pending.csv"

@router.get("/orders")
def orders_endpoint(
    statuses: Optional[str] = Query(default=None, description="Comma-separated statuses"),
    lab_id: Optional[str] = None,
    all: Optional[bool] = Query(default=True, alias="all", description="Fetch all pages"),
    page_limit: Optional[int] = Query(default=None),
    fast: Optional[int] = Query(default=0, description="Use fast parallel fetch with local filtering"),
    per_page: Optional[int] = Query(default=None, description="Override page size for fast mode"),
    workers: Optional[int] = Query(default=None, description="Override max workers for fast mode"),
):
    desired: List[str] | None = None
    if statuses:
        desired = [s.strip() for s in statuses.split(",") if s.strip()]
    else:
        # Default to only ORDERED requests if no statuses provided
        desired = ["ORDERED"]
    if fast:
        orders, meta = quartzy_mod.quartzy_fetch_order_requests_fast(statuses=desired, lab_id=lab_id, per_page=per_page, max_workers=workers)
    else:
        orders, meta = quartzy_mod.quartzy_fetch_order_requests(
            statuses=desired,
            lab_id=lab_id,
            all_pages=bool(all),
            page_limit=page_limit,
        )
    # include lightweight debug snapshot
    dbg = {
    "last_orders_status": quartzy_mod.quartzy_service.debug.get("last_orders_status"),
    "last_orders_http": quartzy_mod.quartzy_service.debug.get("last_orders_http"),
    "last_orders_query_params": quartzy_mod.quartzy_service.debug.get("last_orders_query_params"),
    "observed_statuses": quartzy_mod.quartzy_service.debug.get("observed_statuses"),
    }
    return {"orders": orders, "meta": meta, "debug": dbg}

@router.post("/cache/clear")
def clear_quartzy_cache():
    """Clear Quartzy service in-memory caches (orders, labs, types, inventory_search)."""
    ok = False
    try:
        ok = bool(getattr(quartzy_mod, "quartzy_service").clear_caches())
    except Exception:
        ok = False
    status = 200 if ok else 500
    return JSONResponse(status_code=status, content={"cleared": ok})

@router.get("/orders/{order_id}")
def order_detail_placeholder(order_id: str):
    # Placeholder for future detailed endpoint
    return {"status": 410, "message": "Order detail temporarily unavailable", "id": order_id}

@router.get("/lab/locations")
def lab_locations(
    pages: int = Query(default=5, ge=1, le=20),
    refresh: Optional[int] = Query(default=0),
    lab_id: Optional[str] = None,
    per_page: Optional[int] = Query(default=None, description="Override page size for inventory scan"),
    workers: Optional[int] = Query(default=None, description="Override max workers for inventory scan"),
):
    # Switch to fast collector; 'pages' kept for backward compat, but ignored
    # Ensure we default to configured lab_id when not provided by client
    eff_lab_id = lab_id or getattr(quartzy_mod.quartzy_service, 'lab_id', None)
    res = quartzy_mod.quartzy_collect_lab_locations(lab_id=eff_lab_id, per_page=per_page, refresh=bool(refresh), max_workers=workers)
    # Enrich with configured IDs so the frontend can build correct app URLs
    try:
        if isinstance(res, dict):
            # Prefer explicitly configured group_id when available
            cfg_group_id = getattr(quartzy_mod.quartzy_service, 'group_id', None)
            if cfg_group_id and not res.get("group_id"):
                res["group_id"] = str(cfg_group_id)
            # Also include effective lab_id if not already in payload
            if eff_lab_id and not res.get("lab_id"):
                res["lab_id"] = str(eff_lab_id)
            # Include UI preference for using the explicit "/inventory/new" form
            # Apply prefer_new_form only for the configured lab id from .env
            try:
                cfg_lab_id = getattr(quartzy_mod.quartzy_service, 'lab_id', None)
                pref_new = bool(getattr(quartzy_mod.quartzy_service, 'prefer_new_form', False))
                if cfg_lab_id is not None and eff_lab_id is not None:
                    pref_new = pref_new and (str(eff_lab_id) == str(cfg_lab_id))
                else:
                    # If no eff_lab_id was provided, treat as configured lab context
                    pref_new = pref_new and (eff_lab_id is None)
            except Exception:
                pref_new = False
            res["prefer_new_form"] = pref_new
    except Exception:
        pass
    return res

@router.post("/inventory/match")
def inventory_match(payload: dict):
    # Ensure lab_id falls back to configured value if absent
    try:
        if payload is None:
            payload = {}
        if not payload.get("lab_id"):
            cfg_lab = getattr(quartzy_mod.quartzy_service, 'lab_id', None)
            if cfg_lab:
                payload = {**payload, "lab_id": cfg_lab}
    except Exception:
        pass
    return quartzy_mod.quartzy_find_inventory_match(payload)

@router.post("/inventory/create")
def inventory_create(payload: dict, fallback: str = Query(default="ui", description="Choose fallback if no match: ui|csv|auto|none (default ui)")):
    """Create or update (if exists) an inventory item.
    Behavior:
      - Tries to find an existing item; if found and quantity provided, updates quantity.
      - Else tries API create with robust variants.
      - If create fails and fallback==ui and Playwright add is available, start UI job and return op_id.
      - If create fails and fallback==csv (or auto without UI), append to pending CSV queue.
    """
    # Prefer not to use API create by default; router passes allow_api_create=False explicitly.
    res = quartzy_mod.quartzy_create_inventory(payload, allow_api_create=False)
    if res.get("created") or res.get("updated"):
        # Distinct message + debug for API paths
        msg = None
        dbg = {}
        if res.get("updated"):
            msg = "Inventory updated via Quartzy API"
            upd = res.get("update_result") or res.get("result") or {}
            # Build item link if we can extract id from match
            item_id = None
            try:
                mt = res.get("match") or {}
                it = mt.get("item") or {}
                item_id = it.get("id") or it.get("inventory_item_id") or it.get("raw_id")
            except Exception:
                item_id = None
            item_link = quartzy_mod.quartzy_service.build_quartzy_item_link(str(item_id)) if item_id else None
            link_vars = quartzy_mod.quartzy_service.build_quartzy_item_link_variants(str(item_id)) if item_id else {}
            dbg = {
                "path": upd.get("path"),
                "method": upd.get("method"),
                "http": upd.get("http"),
                "attempts": upd.get("attempts"),
                "request": upd.get("request"),
                "response": upd.get("response"),
                "item_id": item_id,
                "item_link": item_link,
                "item_link_variants": link_vars,
            }
        elif res.get("created"):
            msg = "Inventory item created via Quartzy API"
            crt = res.get("result") or res
            # Try to infer new id
            new_id = None
            try:
                resp = (crt or {}).get("response") or {}
                if isinstance(resp, dict):
                    for k in ("id", "inventory_item", "item", "data"):
                        v = resp.get(k)
                        if isinstance(v, dict) and v.get("id"):
                            new_id = v.get("id"); break
                        if k == "id" and v:
                            new_id = v; break
            except Exception:
                new_id = None
            item_link = quartzy_mod.quartzy_service.build_quartzy_item_link(str(new_id)) if new_id else None
            link_vars = quartzy_mod.quartzy_service.build_quartzy_item_link_variants(str(new_id)) if new_id else {}
            dbg = {
                "path": crt.get("path"),
                "http": crt.get("http"),
                "attempts": crt.get("attempts"),
                "request": crt.get("request"),
                "response": crt.get("response"),
                "item_id": new_id,
                "item_link": item_link,
                "item_link_variants": link_vars,
            }
        enriched = {**res, "message": msg, "debug": dbg}
        return enriched
    # Creation failed: choose fallback
    mode = (fallback or "auto").strip().lower()
    # If service signals UI add is required, force UI path when available
    needs_ui = bool(res.get("needs_ui_add"))
    # UI fallback if requested and available
    if (mode in ("auto", "ui") or needs_ui) and start_auto_add:
        try:
            job_input = {
                "item_name": payload.get("item_name"),
                "vendor_name": payload.get("vendor_name"),
                "catalog_number": payload.get("catalog_number"),
                "quantity": payload.get("quantity"),
                "location": payload.get("location"),
                "sub_location": payload.get("sub_location"),
                "lab_id": payload.get("lab_id"),
            }
            op_id = start_auto_add(job_input)
            return {
                "created": False,
                "queued_ui_add": True,
                "op_id": op_id,
                "prefill_url": res.get("prefill_url"),
                "prefill_url_variants": quartzy_mod.quartzy_service.build_quartzy_add_link_variants(
                    item_name=str(payload.get("item_name") or ""),
                    vendor_name=payload.get("vendor_name"),
                    catalog_number=payload.get("catalog_number"),
                    location=payload.get("location"),
                    lab_id=payload.get("lab_id"),
                ),
                "result": res,
                "message": "No inventory match found. Queued Quartzy UI auto-add.",
                "debug": {"job_input": job_input, "needs_ui_add": needs_ui},
            }
        except Exception as e:
            # Fall through to CSV if auto
            if mode == "ui" or needs_ui:
                return JSONResponse(status_code=500, content={
                    "created": False,
                    "error": f"ui_fallback_failed:{e}",
                    "prefill_url": res.get("prefill_url"),
                    "result": res,
                    "message": "UI auto-add could not be started",
                    "debug": {"exception": str(e)},
                })
    # CSV fallback if requested or auto-without-UI
    if mode in ("auto", "csv"):
        try:
            from app.services.quartzy_service import quartzy_append_pending_inventory
            fields = {
                "item_name": str(payload.get("item_name", "")),
                "vendor_name": payload.get("vendor_name", ""),
                "catalog_number": payload.get("catalog_number", ""),
                "quantity": payload.get("quantity", ""),
                "location": payload.get("location", ""),
                "sub_location": payload.get("sub_location", ""),
                "lab_id": payload.get("lab_id", ""),
                "requested_at": payload.get("requested_at", ""),
                "requested_by": payload.get("requested_by", ""),
                "notes": payload.get("notes", ""),
            }
            row = quartzy_append_pending_inventory(PENDING_INV_CSV, fields)
            return {"created": False, "queued_csv": True, "file": PENDING_INV_CSV, "row": row, "prefill_url": res.get("prefill_url"), "result": res}
        except Exception as e:
            return JSONResponse(status_code=500, content={"created": False, "queued_csv": False, "error": str(e), "result": res})
    # No fallback
    return JSONResponse(status_code=502, content={"created": False, "prefill_url": res.get("prefill_url"), "result": res})

@router.post("/inventory/update")
def inventory_update(payload: dict):
    """Update existing inventory quantity (absolute or additive). Accepts:
    {"item_id": "uuid", "new_quantity": 42} OR {"item_id": "uuid", "add_quantity": 5}
    Returns attempts log for debugging when failures occur.
    """
    item_id = payload.get("item_id") or payload.get("id")
    if not item_id:
        return {"updated": False, "error": "item_id required"}
    new_quantity = payload.get("new_quantity")
    add_quantity = payload.get("add_quantity")
    # If additive provided but no absolute quantity, fetch current then compute target
    qty_to_set = None
    if new_quantity is not None:
        qty_to_set = new_quantity
    elif add_quantity is not None:
        try:
            current = quartzy_mod.quartzy_service.get_inventory_item(str(item_id))
            # Try to extract quantity from fields
            raw = current.get("item") or current.get("response") or current
            base_obj = raw
            if isinstance(raw, dict):
                for k in ("inventory_item", "item", "data"):
                    if isinstance(raw.get(k), dict):
                        base_obj = raw.get(k)
                        break
            existing = None
            if isinstance(base_obj, dict):
                for k in ("quantity", "quantity_in_stock", "qty", "in_stock_quantity"):
                    if isinstance(base_obj.get(k), (int, float, str)):
                        try:
                            existing = int(str(base_obj.get(k)).split(".")[0])
                            break
                        except Exception:
                            pass
            if existing is None:
                existing = 0
            qty_to_set = int(existing) + int(add_quantity)
        except Exception as e:
            return {"updated": False, "error": f"failed_to_fetch_current:{e}"}
    if qty_to_set is None:
        return {"updated": False, "error": "no_quantity_provided"}
    # Delegate to service multi-strategy updater
    result = quartzy_mod.quartzy_service.update_inventory_item(item_id=str(item_id), quantity=str(qty_to_set))
    # Attach computed target
    result["target_quantity"] = qty_to_set
    return result

@router.post("/inventory/find")
def inventory_find(payload: dict, limit: int = 5, auto_add_if_not_found: int = Query(default=1, ge=0, le=1)):
    """Return top candidates for interactive selection. If no match (or search yields nothing)
    and auto_add_if_not_found=1, start a Playwright auto-add job using available form fields
    (excluding received_by and comments) and return the operation id for polling.
    """
    item_name = payload.get("item_name")
    vendor_name = payload.get("vendor_name")
    catalog_number = payload.get("catalog_number")
    lab_id = payload.get("lab_id") or getattr(quartzy_mod.quartzy_service, 'lab_id', None)
    res = quartzy_mod.quartzy_service.find_inventory_candidates(
        item_name=item_name,
        vendor_name=vendor_name,
        catalog_number=catalog_number,
        lab_id=lab_id,
        limit=limit,
    )
    # Attach the lab_id we effectively used and inventory debug for clarity
    try:
        inv_debug = {
            "lab_id_used": lab_id,
            "endpoint_selected": quartzy_mod.quartzy_service.debug.get("inventory_endpoint_selected"),
            "endpoint_attempts": quartzy_mod.quartzy_service.debug.get("inventory_endpoint_probe_attempts"),
            "fetch_config": quartzy_mod.quartzy_service.debug.get("inventory_fetch_config"),
        }
        if isinstance(res, dict):
            res = {**res, "debug": {**(res.get("debug") or {}), "inventory": inv_debug}}
    except Exception:
        pass
    # If not found and auto_add requested, trigger UI add job using available context
    if not (res or {}).get("found") and auto_add_if_not_found and start_auto_add:
        # Build a prefill URL as well for transparency/debug
        try:
            prefill = quartzy_mod.quartzy_service.build_quartzy_add_link(
                item_name=str(item_name or ""),
                vendor_name=vendor_name,
                catalog_number=catalog_number,
                location=(payload.get("location") or None),
                lab_id=lab_id,
            )
            prefill_vars = quartzy_mod.quartzy_service.build_quartzy_add_link_variants(
                item_name=str(item_name or ""),
                vendor_name=vendor_name,
                catalog_number=catalog_number,
                location=(payload.get("location") or None),
                lab_id=lab_id,
            )
        except Exception:
            prefill = None
            prefill_vars = {}
        try:
            job_input = {
                "item_name": item_name,
                "vendor_name": vendor_name,
                "catalog_number": catalog_number,
                "quantity": payload.get("quantity"),
                "location": payload.get("location"),
                "sub_location": payload.get("sub_location"),
                "lab_id": lab_id,
            }
            op_id = start_auto_add(job_input)
            return {
                "found": False,
                "queued_ui_add": True,
                "op_id": op_id,
                "prefill_url": prefill,
                "prefill_url_variants": prefill_vars,
                "message": "No inventory candidates found. Queued Quartzy UI auto-add.",
                "debug": {"job_input": job_input, "query": {"item_name": item_name, "vendor_name": vendor_name, "catalog_number": catalog_number}},
            }
        except Exception as e:
            # If UI job fails to start, return original not-found with prefill link for manual action
            return {"found": False, "error": f"ui_auto_add_failed:{e}", "prefill_url": prefill, "prefill_url_variants": prefill_vars, "message": "UI auto-add could not be started", "debug": {"exception": str(e)}}
    return res

@router.post("/inventory/ensure")
def inventory_ensure(payload: dict):
    item_name = str(payload.get("item_name") or "")
    vendor_name = payload.get("vendor_name")
    catalog_number = payload.get("catalog_number")
    location = payload.get("location")
    lab_id = payload.get("lab_id")
    return quartzy_mod.quartzy_service.ensure_item_in_inventory(item_name=item_name, vendor_name=vendor_name, catalog_number=catalog_number, location=location, lab_id=lab_id)

@router.post("/inventory/pending/add")
def inventory_pending_add(payload: Dict[str, Any]):
    # Append item to a local CSV queue for manual import in Quartzy
    fields = {
        "item_name": payload.get("item_name", ""),
        "vendor_name": payload.get("vendor_name", ""),
        "catalog_number": payload.get("catalog_number", ""),
        "quantity": payload.get("quantity", ""),
        "location": payload.get("location", ""),
        "sub_location": payload.get("sub_location", ""),
        "lab_id": payload.get("lab_id", ""),
        "requested_at": payload.get("requested_at", ""),
        "requested_by": payload.get("requested_by", ""),
        "notes": payload.get("notes", ""),
    }
    try:
        from app.services.quartzy_service import quartzy_append_pending_inventory
        row = quartzy_append_pending_inventory(PENDING_INV_CSV, fields)
        return {"queued": True, "file": PENDING_INV_CSV, "row": row}
    except Exception as e:
        return JSONResponse(status_code=500, content={"queued": False, "error": str(e)})

@router.get("/inventory/pending/csv")
def inventory_pending_csv():
    if not os.path.exists(PENDING_INV_CSV):
        # Return an empty CSV with headers so users can download a template
        try:
            from app.services.quartzy_service import quartzy_ensure_pending_inventory_csv
            quartzy_ensure_pending_inventory_csv(PENDING_INV_CSV)
        except Exception:
            pass
    if not os.path.exists(PENDING_INV_CSV):
        return JSONResponse(status_code=404, content={"error": "No pending inventory CSV found"})
    return FileResponse(PENDING_INV_CSV, media_type="text/csv", filename=os.path.basename(PENDING_INV_CSV))

# -------- Auto-add via Playwright (optional) --------
try:
    from app.services.quartzy_auto_add_service import start_auto_add, get_job
except Exception:  # pragma: no cover
    start_auto_add = None  # type: ignore
    get_job = None  # type: ignore

@router.post("/inventory/auto_add/start")
def quartzy_auto_add_start(payload: dict = Body(...)):
    if not start_auto_add:
        return JSONResponse(status_code=400, content={"error": "auto_add_unavailable (playwright not installed)"})
    op_id = start_auto_add(payload or {})
    return {"op_id": op_id}

@router.get("/inventory/auto_add/status")
def quartzy_auto_add_status(op_id: str = Query(...)):
    if not get_job:
        return JSONResponse(status_code=400, content={"error": "auto_add_unavailable"})
    return get_job(op_id)

# -------- Auto-update quantity via Playwright (optional) --------
try:
    from app.services.quartzy_auto_update_service import start_auto_update_quantity, get_job as get_update_job
except Exception:  # pragma: no cover
    start_auto_update_quantity = None  # type: ignore
    get_update_job = None  # type: ignore

@router.post("/inventory/auto_update/start")
def quartzy_auto_update_start(payload: dict = Body(...)):
    if not start_auto_update_quantity:
        return JSONResponse(status_code=400, content={"error": "auto_update_unavailable (playwright not installed)"})
    op_id = start_auto_update_quantity(payload or {})
    return {"op_id": op_id}

@router.get("/inventory/auto_update/status")
def quartzy_auto_update_status(op_id: str = Query(...)):
    if not get_update_job:
        return JSONResponse(status_code=400, content={"error": "auto_update_unavailable"})
    return get_update_job(op_id)

# -------- Order adjust via Playwright (optional) --------
try:
    from app.services.quartzy_order_adjust_service import start_adjust_order_quantity, get_job as get_adjust_job
except Exception:  # pragma: no cover
    start_adjust_order_quantity = None  # type: ignore
    get_adjust_job = None  # type: ignore

@router.post("/order/adjust/start")
def quartzy_order_adjust_start(payload: dict = Body(...)):
    if not start_adjust_order_quantity:
        return JSONResponse(status_code=400, content={"error": "order_adjust_unavailable (playwright not installed)"})
    op_id = start_adjust_order_quantity(payload or {})
    return {"op_id": op_id}

@router.get("/order/adjust/status")
def quartzy_order_adjust_status(op_id: str = Query(...)):
    if not get_adjust_job:
        return JSONResponse(status_code=400, content={"error": "order_adjust_unavailable"})
    return get_adjust_job(op_id)
