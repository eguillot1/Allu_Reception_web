"""Quartzy inventory auto-add service.
Uses a hybrid approach: check for existing inventory via Quartzy service; if not found,
optionally launch a headless browser via Playwright to add the item using the Quartzy UI.

This runs jobs in a background thread and exposes simple in-memory job status tracking.
"""
from __future__ import annotations
import os
import threading
import uuid
import time
from typing import Dict, Any, List

try:
    from playwright.sync_api import sync_playwright  # type: ignore
except Exception:  # Playwright not installed or browsers not set up
    sync_playwright = None  # type: ignore

from app.services import quartzy_service as _qs_mod

JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()

def _set_job(op_id: str, **kwargs):
    with JOBS_LOCK:
        job = JOBS.setdefault(op_id, {})
        job.update(kwargs)
        job.setdefault("updated_at", time.time())
        job["updated_at"] = time.time()
        return job

def _append_log(op_id: str, msg: str, **meta):
    with JOBS_LOCK:
        job = JOBS.setdefault(op_id, {})
        lg: List[Dict[str, Any]] = job.setdefault("log", [])  # type: ignore
        lg.append({
            "t": round(time.time(), 3),
            "msg": msg,
            **({k: v for k, v in (meta or {}).items() if v is not None})
        })
        job["updated_at"] = time.time()
        return lg

def get_job(op_id: str) -> Dict[str, Any]:
    with JOBS_LOCK:
        return dict(JOBS.get(op_id, {}))

def start_auto_add(item: Dict[str, Any]) -> str:
    """Start an auto-add job; returns op_id immediately."""
    op_id = str(uuid.uuid4())
    clean = {k: (item.get(k) or "") for k in [
        "name","vendor","catalog_number","quantity","unit_size","location","sublocation","notes"
    ]}
    _set_job(op_id, status="queued", item=clean)
    _append_log(op_id, "job_queued", payload=clean)
    t = threading.Thread(target=_worker, args=(op_id, item), daemon=True)
    t.start()
    return op_id

def _worker(op_id: str, item: Dict[str, Any]):
    _set_job(op_id, status="checking")
    _append_log(op_id, "checking_existing")
    name = (item.get("name") or item.get("item_name") or "").strip()
    vendor = (item.get("vendor") or item.get("supplier") or "").strip()
    catalog = (item.get("catalog_number") or "").strip()
    location = (item.get("location") or "").strip()
    sub_location = (item.get("sublocation") or item.get("sub_location") or "").strip()
    notes = (item.get("notes") or item.get("comments") or "").strip()
    quantity = str(item.get("quantity") or "").strip()
    unit_size = str(item.get("unit_size") or "").strip()

    try:
        _qs = _qs_mod.quartzy_service
        # First try to find best match via API
        best = None
        try:
            best = _qs.find_inventory_match_best(item_name=name, vendor_name=vendor, catalog_number=catalog)
        except Exception:
            best = None
        if best and best.get("found"):
            inv_id = (best.get("item") or {}).get("id")
            url = _qs.build_quartzy_item_link(inv_id) if inv_id else None
            _set_job(op_id, status="already_exists", success=True, item_id=inv_id, item_url=url)
            _append_log(op_id, "existing_item_found", item_id=inv_id, item_url=url)
            return

        # If not found and Playwright available, attempt UI creation
        if sync_playwright is None:
            _set_job(op_id, status="error", success=False, error="playwright_not_available")
            _append_log(op_id, "playwright_missing")
            return
        email = os.getenv("QUARTZY_EMAIL")
        password = os.getenv("QUARTZY_PASSWORD")
        if not email or not password:
            _set_job(op_id, status="error", success=False, error="missing_quartzy_credentials")
            _append_log(op_id, "missing_credentials")
            return

        headless = (os.getenv("QUARTZY_PLAYWRIGHT_HEADLESS", "1") not in ("0","false","False"))
        _set_job(op_id, status="launching_browser", headless=headless)
        _append_log(op_id, "launching_browser", headless=headless)
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)  # requires browser installed via `playwright install chromium`
            context = browser.new_context()
            page = context.new_page()
            try:
                page.goto("https://app.quartzy.com/login")
                _append_log(op_id, "goto_login", url=page.url)
                page.fill('input[name="email"]', email)
                page.fill('input[name="password"]', password)
                page.click('button[type="submit"]')
                try:
                    page.wait_for_url("**/dashboard", timeout=60000)
                except Exception:
                    # Some tenants land on /inventory directly after SSO flow
                    _append_log(op_id, "dashboard_wait_timeout", current_url=page.url)

                page.goto("https://app.quartzy.com/inventory")
                _append_log(op_id, "goto_inventory", url=page.url)
                page.wait_for_load_state("networkidle")

                # Try robust ways to open the Add Item dialog
                clicked = False
                selectors = [
                    'button:has-text("Add Item")',
                    'text="Add Item"',
                    'role=button[name="Add Item"]',
                    'button:has(svg) >> nth=0'  # heuristic if a plus icon button
                ]
                for sel in selectors:
                    try:
                        page.click(sel, timeout=4000)
                        clicked = True
                        _append_log(op_id, "clicked_add_item", selector=sel)
                        break
                    except Exception:
                        _append_log(op_id, "click_retry_failed", selector=sel)
                        continue
                if not clicked:
                    raise RuntimeError("could_not_open_add_item_dialog")
                page.wait_for_selector('input[name="name"]', timeout=10000)
                _append_log(op_id, "add_dialog_opened")

                page.fill('input[name="name"]', name)
                if vendor:
                    page.fill('input[name="vendor"]', vendor)
                if catalog:
                    page.fill('input[name="catalog_number"]', catalog)
                if quantity:
                    try:
                        page.fill('input[name="quantity"]', quantity)
                    except Exception:
                        _append_log(op_id, "fill_quantity_failed")
                        pass
                if unit_size:
                    try:
                        page.fill('input[name="unit_size"]', unit_size)
                    except Exception:
                        _append_log(op_id, "fill_unit_size_failed")
                        pass
                if notes:
                    try:
                        page.fill('textarea[name="notes"]', notes)
                    except Exception:
                        _append_log(op_id, "fill_notes_failed")
                        pass
                # Location dropdowns (best-effort; selectors may vary)
                try:
                    if location:
                        page.click('div[role="button"]:has-text("Location")')
                        page.fill('input[placeholder="Search"]', location)
                        page.keyboard.press("Enter")
                    if sub_location:
                        page.click('div[role="button"]:has-text("Sublocation")')
                        page.fill('input[placeholder="Search"]', sub_location)
                        page.keyboard.press("Enter")
                    _append_log(op_id, "set_location_attempted", location=location, sub_location=sub_location)
                except Exception as e:
                    _append_log(op_id, "set_location_failed", error=str(e))
                    pass

                page.click('button:has-text("Save")')
                _append_log(op_id, "clicked_save")
                # Try to wait for a success indicator
                try:
                    page.wait_for_selector('text=Item added', timeout=15000)
                    _append_log(op_id, "save_success_indicator_seen")
                except Exception:
                    _append_log(op_id, "save_success_indicator_missing")
                    pass

            finally:
                try:
                    # Capture a screenshot for diagnostics
                    try:
                        os.makedirs("uploads", exist_ok=True)
                        shot = os.path.join("uploads", f"auto_add_{op_id}_final.png")
                        page.screenshot(path=shot, full_page=True)
                        _append_log(op_id, "screenshot_saved", path=shot)
                    except Exception as e:
                        _append_log(op_id, "screenshot_failed", error=str(e))
                    browser.close()
                except Exception:
                    pass

        # Post-create: attempt to locate the item again
        time.sleep(2)
        try:
            best2 = _qs.find_inventory_match_best(item_name=name, vendor_name=vendor, catalog_number=catalog)
        except Exception as e:
            _append_log(op_id, "post_create_search_failed", error=str(e))
            best2 = None
        if best2 and best2.get("found"):
            inv_id = (best2.get("item") or {}).get("id")
            url = _qs.build_quartzy_item_link(inv_id) if inv_id else None
            _set_job(op_id, status="created", success=True, item_id=inv_id, item_url=url)
            _append_log(op_id, "created_ok", item_id=inv_id, item_url=url)
            return
        _set_job(op_id, status="unknown", success=False, error="post_create_not_found")
        _append_log(op_id, "post_create_not_found")
    except Exception as e:  # pragma: no cover
        _set_job(op_id, status="error", success=False, error=str(e))
        _append_log(op_id, "job_exception", error=str(e))

__all__ = ["start_auto_add","get_job"]
