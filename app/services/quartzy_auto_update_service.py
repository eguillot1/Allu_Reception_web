"""Quartzy inventory auto-update (quantity) service.
Runs a Playwright browser to update the quantity of an existing inventory item
when the official API doesn't support updates for the tenant.

Exports two helpers compatible with the auto-add service style:
- start_auto_update_quantity(payload) -> op_id
- get_job(op_id) -> latest job dict

Payload shape (inputs):
- item_id: string (required)
- target_quantity: int (required) absolute quantity to set
- item_url: optional, direct app URL for the item; if missing we will build it
  from the configured group/lab id using quartzy_service.build_quartzy_item_link.
"""
from __future__ import annotations
import os
import threading
import uuid
import time
from typing import Dict, Any, List, Optional

try:
    from playwright.sync_api import sync_playwright  # type: ignore
except Exception:
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

def start_auto_update_quantity(payload: Dict[str, Any]) -> str:
    """Start a background job to update an inventory item's quantity via the Quartzy UI."""
    op_id = str(uuid.uuid4())
    item_id = str(payload.get("item_id") or "").strip()
    target_qty = payload.get("target_quantity")
    item_url = payload.get("item_url") or None
    try:
        if not item_id:
            raise ValueError("missing_item_id")
        if target_qty is None:
            raise ValueError("missing_target_quantity")
        try:
            target_qty = int(str(target_qty).split(".")[0])
        except Exception:
            raise ValueError("invalid_target_quantity")
    except Exception as e:
        _set_job(op_id, status="error", success=False, error=str(e))
        return op_id

    # Build item URL if not provided
    if not item_url:
        try:
            item_url = _qs_mod.quartzy_service.build_quartzy_item_link(item_id)
        except Exception:
            item_url = None

    clean = {"item_id": item_id, "target_quantity": target_qty, "item_url": item_url}
    _set_job(op_id, status="queued", request=clean)
    _append_log(op_id, "job_queued", payload=clean)

    t = threading.Thread(target=_worker, args=(op_id, item_id, target_qty, item_url), daemon=True)
    t.start()
    return op_id

def _worker(op_id: str, item_id: str, target_quantity: int, item_url: Optional[str]):
    _set_job(op_id, status="starting")
    _append_log(op_id, "starting_worker")
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
    # Build URL if still absent
    if not item_url:
        try:
            item_url = _qs_mod.quartzy_service.build_quartzy_item_link(item_id)
        except Exception:
            item_url = "https://app.quartzy.com/inventory"  # safe fallback

    headless = (os.getenv("QUARTZY_PLAYWRIGHT_HEADLESS", "1") not in ("0","false","False"))
    _set_job(op_id, status="launching_browser", headless=headless)
    _append_log(op_id, "launching_browser", headless=headless)
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context()
            page = context.new_page()
            try:
                # Login
                page.goto("https://app.quartzy.com/login")
                _append_log(op_id, "goto_login", url=page.url)
                page.fill('input[name="email"]', email)
                page.fill('input[name="password"]', password)
                page.click('button[type="submit"]')
                try:
                    page.wait_for_url("**/dashboard", timeout=60000)
                except Exception:
                    _append_log(op_id, "dashboard_wait_timeout", current_url=page.url)

                # Navigate to item page
                page.goto(str(item_url))
                _append_log(op_id, "goto_item", url=page.url)
                page.wait_for_load_state("networkidle")

                # Try to enter edit mode if required
                edit_clicked = False
                for sel in [
                    'button:has-text("Edit")',
                    'role=button[name="Edit"]',
                    'text=Edit',
                    'button[aria-label="Edit"]',
                ]:
                    try:
                        page.click(sel, timeout=3000)
                        edit_clicked = True
                        _append_log(op_id, "clicked_edit", selector=sel)
                        break
                    except Exception:
                        continue
                if not edit_clicked:
                    _append_log(op_id, "edit_button_not_found")

                # Fill quantity: try multiple robust selectors
                qty_set = False
                candidate_inputs = [
                    'input[name="quantity"]',
                    'input[aria-label="Quantity"]',
                    'input[id*="quantity" i]',
                ]
                for sel in candidate_inputs:
                    try:
                        page.fill(sel, str(int(target_quantity)))
                        qty_set = True
                        _append_log(op_id, "quantity_filled", selector=sel, value=int(target_quantity))
                        break
                    except Exception:
                        continue
                if not qty_set:
                    # Attempt to focus the input near a Quantity label
                    try:
                        label = page.locator('label:has-text("Quantity")')
                        if label.count() > 0:
                            # assume next input in DOM
                            el = label.nth(0).locator('xpath=following::*[self::input or self::textarea][1]')
                            el.fill(str(int(target_quantity)))
                            qty_set = True
                            _append_log(op_id, "quantity_filled_via_label", value=int(target_quantity))
                    except Exception as e:
                        _append_log(op_id, "quantity_fill_failed", error=str(e))

                # Click Save/Update
                saved = False
                for sel in [
                    'button:has-text("Save")',
                    'role=button[name="Save"]',
                    'button:has-text("Update")',
                ]:
                    try:
                        page.click(sel, timeout=4000)
                        saved = True
                        _append_log(op_id, "clicked_save", selector=sel)
                        break
                    except Exception:
                        continue
                if not saved:
                    _append_log(op_id, "save_click_not_found")

                # Wait for some success signal or page to settle
                try:
                    page.wait_for_load_state("networkidle")
                except Exception:
                    pass
                try:
                    page.wait_for_timeout(1500)
                except Exception:
                    pass

                # Done
                _set_job(op_id, status="updated", success=True, item_id=item_id, item_url=item_url, new_quantity=int(target_quantity))
                _append_log(op_id, "update_done", item_id=item_id, item_url=item_url, new_quantity=int(target_quantity))
            finally:
                # Capture screenshot for diagnostics
                try:
                    os.makedirs("uploads", exist_ok=True)
                    shot = os.path.join("uploads", f"auto_update_{op_id}_final.png")
                    try:
                        page.screenshot(path=shot, full_page=True)
                        _append_log(op_id, "screenshot_saved", path=shot)
                    except Exception as se:
                        _append_log(op_id, "screenshot_failed", error=str(se))
                except Exception:
                    pass
                try:
                    browser.close()
                except Exception:
                    pass
    except Exception as e:
        _set_job(op_id, status="error", success=False, error=str(e))
        _append_log(op_id, "job_exception", error=str(e))

__all__ = ["start_auto_update_quantity", "get_job"]
