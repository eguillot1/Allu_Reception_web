"""Quartzy Order Request adjust (quantity) service via UI automation.
Adjusts an order request's expected quantity to match remaining (ordered - received)
when the public API does not provide a way to edit quantity.

Exports:
- start_adjust_order_quantity(payload) -> op_id
- get_job(op_id) -> job dict

Payload fields:
- order_id: str (required)
- target_quantity: int (required)  # desired remaining expected quantity
"""
from __future__ import annotations
import os
import threading
import uuid
import time
from typing import Dict, Any, Optional
import traceback
import sys
import asyncio

try:
    from playwright.sync_api import sync_playwright  # type: ignore
except Exception:
    sync_playwright = None  # type: ignore

from app.services import quartzy_service as _qs_mod

JOBS: Dict[str, Dict[str, Any]] = {}
_LOCK = threading.Lock()

def _set_job(op_id: str, **kwargs):
    with _LOCK:
        job = JOBS.setdefault(op_id, {})
        job.update(kwargs)
        job["updated_at"] = time.time()
        return job

def _append(op_id: str, msg: str, **meta):
    with _LOCK:
        job = JOBS.setdefault(op_id, {})
        lg = job.setdefault("log", [])  # type: ignore
        lg.append({"t": round(time.time(), 3), "msg": msg, **meta})
        job["updated_at"] = time.time()
        return lg

def get_job(op_id: str) -> Dict[str, Any]:
    with _LOCK:
        return dict(JOBS.get(op_id, {}))

def _snap(tag: str, page, op_id: str, tracing_enabled: bool=False):
    # Skip screenshots by default for speed unless explicitly enabled or tracing is on
    if os.getenv("QUARTZY_PLAYWRIGHT_SNAP", "0") not in ("1", "true", "True") and not tracing_enabled:
        return
    try:
        os.makedirs("uploads", exist_ok=True)
        path = os.path.join("uploads", f"order_adjust_{op_id}_{tag}.png")
        page.screenshot(path=path, full_page=True)
        _append(op_id, "screenshot_saved", tag=tag, path=path)
    except Exception as se:
        _append(op_id, "screenshot_failed", tag=tag, error=str(se))

def start_adjust_order_quantity(payload: Dict[str, Any]) -> str:
    op_id = str(uuid.uuid4())
    order_id = str(payload.get("order_id") or "").strip()
    target_qty = payload.get("target_quantity")
    try:
        if not order_id:
            raise ValueError("missing_order_id")
        if target_qty is None:
            raise ValueError("missing_target_quantity")
        target_qty = int(str(target_qty).split(".")[0])
        if target_qty < 0:
            target_qty = 0
    except Exception as e:
        _set_job(op_id, status="error", success=False, error=str(e))
        return op_id

    # Try to fetch the order to get app_url to navigate directly
    order_app_url = None
    try:
        res = _qs_mod.quartzy_service.get_order_request(order_id)
        if res and res.get("found"):
            order_app_url = (res.get("order") or {}).get("app_url")
    except Exception:
        pass

    clean = {"order_id": order_id, "target_quantity": target_qty, "app_url": order_app_url}
    _set_job(op_id, status="queued", request=clean)
    _append(op_id, "job_queued", **clean)

    t = threading.Thread(target=_worker, args=(op_id, order_id, target_qty, order_app_url), daemon=True)
    t.start()
    return op_id

def _worker(op_id: str, order_id: str, target_quantity: int, app_url: Optional[str]):
    _set_job(op_id, status="starting")
    _append(op_id, "starting_worker")
    # Windows fix: Playwright needs subprocess support; ensure Selector loop policy
    try:
        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            _append(op_id, "event_loop_policy_set", policy="WindowsProactorEventLoopPolicy")
    except Exception as _elp:
        _append(op_id, "event_loop_policy_set_failed", error=str(_elp) or repr(_elp))
    if sync_playwright is None:
        _set_job(op_id, status="error", success=False, error="playwright_not_available")
        _append(op_id, "playwright_missing")
        return
    email = os.getenv("QUARTZY_EMAIL")
    password = os.getenv("QUARTZY_PASSWORD")
    if not email or not password:
        _set_job(op_id, status="error", success=False, error="missing_quartzy_credentials")
        _append(op_id, "missing_credentials")
        return

    # Build fallback URL if app_url missing; best guess path
    if not app_url:
        try:
            res = _qs_mod.quartzy_service.get_order_request(order_id)
            if res and res.get("found"):
                app_url = (res.get("order") or {}).get("app_url")
        except Exception:
            app_url = None
    if not app_url:
        app_url = "https://app.quartzy.com/orders"

    headless = (os.getenv("QUARTZY_PLAYWRIGHT_HEADLESS", "1") not in ("0","false","False"))
    _set_job(op_id, status="launching_browser", headless=headless)
    _append(op_id, "launching_browser", headless=headless)
    try:
        from playwright.sync_api import TimeoutError as PWTimeout  # type: ignore
        with sync_playwright() as p:
            browser = None
            launch_error = None
            try:
                browser = p.chromium.launch(headless=headless, args=[
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--disable-sync",
                    "--disable-default-apps",
                    "--disable-notifications",
                ])
                _append(op_id, "chromium_launch_ok", channel=None)
            except Exception as e1:
                launch_error = e1
                _append(op_id, "chromium_launch_failed", error=str(e1) or repr(e1))
                try:
                    browser = p.chromium.launch(headless=headless, channel="chrome")
                    _append(op_id, "chromium_launch_ok", channel="chrome")
                except Exception as e2:
                    _append(op_id, "chromium_launch_failed_channel", error=str(e2) or repr(e2))
                    try:
                        browser = p.chromium.launch(headless=headless, args=["--no-sandbox", "--disable-gpu"])  # --no-sandbox mostly Linux
                        _append(op_id, "chromium_launch_ok", channel="default_with_args")
                    except Exception as e3:
                        _append(op_id, "chromium_launch_failed_args", error=str(e3) or repr(e3))
                        raise launch_error
            # Reuse prior session if enabled via env
            reuse_session = os.getenv("QUARTZY_PLAYWRIGHT_REUSE_SESSION", "0") in ("1","true","True")
            storage_state_path = os.getenv("QUARTZY_PLAYWRIGHT_STORAGE", os.path.join("uploads", "quartzy_storage_state.json"))
            if reuse_session:
                try:
                    os.makedirs(os.path.dirname(storage_state_path), exist_ok=True)
                except Exception:
                    pass
                storage_state_arg = storage_state_path if (os.path.exists(storage_state_path)) else None
                context = browser.new_context(storage_state=storage_state_arg)
            else:
                storage_state_arg = None
                context = browser.new_context()
            # Optionally block heavy resources
            if os.getenv("QUARTZY_PLAYWRIGHT_BLOCK_HEAVY", "0") in ("1","true","True"):
                try:
                    def _route_handler(route, request):
                        rtype = request.resource_type
                        url = request.url
                        if rtype in ("image", "media", "font") or ("google-analytics" in url) or ("googletagmanager" in url):
                            return route.abort()
                        return route.continue_()
                    context.route("**/*", _route_handler)  # type: ignore
                except Exception:
                    pass
            # Optional tracing (enable via QUARTZY_PLAYWRIGHT_TRACE=1)
            tracing_enabled = os.getenv("QUARTZY_PLAYWRIGHT_TRACE", "0") in ("1","true","True")
            if tracing_enabled:
                try:
                    context.tracing.start(screenshots=True, snapshots=True, sources=True)  # type: ignore
                    _append(op_id, "tracing_started")
                except Exception as te:
                    _append(op_id, "tracing_start_failed", error=str(te) or repr(te))
            page = context.new_page()
            try:
                page.set_default_timeout(10000)
            except Exception:
                pass

            # Capture page console and page errors for diagnostics
            try:
                page.on("console", lambda m: _append(op_id, "page_console", level=m.type, text=m.text()))  # type: ignore
            except Exception:
                pass
            try:
                page.on("pageerror", lambda e: _append(op_id, "page_error", error=str(e)))  # type: ignore
            except Exception:
                pass

            def _dismiss_banners():
                for sel in [
                    'button:has-text("Accept All")',
                    'button:has-text("Accept")',
                    'button:has-text("Got it")',
                    'button:has-text("I Agree")',
                ]:
                    try:
                        page.locator(sel).first.click(timeout=1000)
                        _append(op_id, "banner_dismissed", selector=sel)
                    except Exception:
                        pass

            def _click_first(selectors, timeout=4000, name="click"):
                for sel in selectors:
                    try:
                        page.locator(sel).first.wait_for(state="visible", timeout=timeout)
                        page.locator(sel).first.click(timeout=timeout)
                        _append(op_id, f"{name}_ok", selector=sel)
                        return True
                    except Exception:
                        continue
                _append(op_id, f"{name}_not_found", tried=selectors)
                return False

            def _wait_first_visible_editable(selectors, total_timeout_ms=15000):
                deadline = time.time() + (total_timeout_ms / 1000.0)
                while time.time() < deadline:
                    for sel in selectors:
                        try:
                            loc = page.locator(sel).first
                            if loc.count() > 0 and loc.is_visible():
                                try:
                                    if hasattr(loc, 'is_editable') and not loc.is_editable():
                                        continue
                                except Exception:
                                    pass
                                return loc
                        except Exception:
                            pass
                    try:
                        page.wait_for_timeout(200)
                    except Exception:
                        pass
                return None

            def _find_quantity_input():
                for sel in [
                    'input[name="quantity"]',
                    'input[aria-label="Quantity"]',
                    'input[aria-label*="Quant" i]',
                    'input[id*="quantity" i]',
                    'input[type="number"]',
                ]:
                    try:
                        el = page.locator(sel).first
                        if el.count() > 0 and el.is_visible():
                            return el
                    except Exception:
                        continue
                try:
                    labels = [
                        'label:has-text("Quantity")',
                        'label:has-text("Quantity expected")',
                        'label:has-text("Quantity ordered")',
                        'label:has-text("Qty")',
                    ]
                    for lab_sel in labels:
                        lab = page.locator(lab_sel).first
                        if lab.count() > 0:
                            el = lab.locator('xpath=following::*[self::input or self::textarea][1]')
                            if el and el.count() > 0 and el.first.is_visible():
                                return el.first
                except Exception:
                    pass
                return None

            def _fill_input_robust(el, value: int):
                try:
                    try:
                        if hasattr(el, 'is_editable') and not el.is_editable():
                            _append(op_id, "quantity_field_not_editable")
                            return False
                    except Exception:
                        pass
                    el.click()
                    try:
                        page.keyboard.down('Control')
                        page.keyboard.press('A')
                        page.keyboard.up('Control')
                    except Exception:
                        pass
                    try:
                        page.keyboard.press('Backspace')
                    except Exception:
                        pass
                    el.type(str(int(value)))
                    try:
                        current = el.input_value()
                        _append(op_id, "quantity_input_value", value=current)
                        return str(int(value)) == str(int(current))
                    except Exception:
                        return True
                except Exception as fe:
                    _append(op_id, "quantity_fill_failed", error=str(fe) or repr(fe))
                    return False

            try:
                # Fast path: if we reuse session and have storage, go directly to the order page
                session_reused = False
                if reuse_session and storage_state_arg:
                    page.goto(str(app_url), wait_until="domcontentloaded")
                    if ("auth0." not in page.url) and ("/login" not in page.url):
                        session_reused = True
                        _append(op_id, "session_reused", url=page.url)
                if not session_reused:
                    # Login (handle Auth0 Universal Login or classic form)
                    page.goto("https://app.quartzy.com/login")
                    _append(op_id, "goto_login", url=page.url)
                    try:
                        page.wait_for_load_state("domcontentloaded", timeout=20000)
                    except Exception:
                        pass
                    # Branch on Auth0 identifier/password steps vs classic
                    try:
                        current_url = page.url
                        if ("auth0." in current_url) or ("/u/login" in current_url):
                            _append(op_id, "auth0_flow_detected", url=current_url)
                            # Step 1: identifier (email)
                            try:
                                email_selectors = [
                                    'input[type="email"]',
                                    'input[name="email"]',
                                    'input#username',
                                    'input[name="username"]',
                                    'input[autocomplete="username"]',
                                    'input[placeholder*="email" i]'
                                ]
                                el_email = _wait_first_visible_editable(email_selectors, total_timeout_ms=15000)
                                if el_email is None:
                                    raise RuntimeError("email_field_not_visible")
                                el_email.click()
                                el_email.fill(email)
                                _append(op_id, "auth0_email_filled")
                            except Exception as ee:
                                _append(op_id, "auth0_email_fill_failed", error=str(ee) or repr(ee))
                            # Continue to password step
                            cont_clicked = False
                            for sel in ['button[type="submit"]', 'button:has-text("Continue")', 'button:has-text("Continue with Email")']:
                                try:
                                    page.locator(sel).first.click(timeout=8000)
                                    cont_clicked = True
                                    _append(op_id, "auth0_continue_clicked", selector=sel)
                                    break
                                except Exception:
                                    continue
                            if not cont_clicked:
                                _append(op_id, "auth0_continue_not_found")
                            # Step 2: password
                            try:
                                # Wait for password field to appear (password step url or field)
                                page.wait_for_timeout(800)
                                pwd_selectors = [
                                    'input[name="password"]',
                                    'input[type="password"]',
                                    'input[autocomplete="current-password"]',
                                    'input[placeholder*="password" i]'
                                ]
                                el_pwd = _wait_first_visible_editable(pwd_selectors, total_timeout_ms=15000)
                                if el_pwd is None:
                                    raise RuntimeError("password_field_not_visible")
                                el_pwd.click()
                                el_pwd.fill(password)
                                _append(op_id, "auth0_password_filled")
                            except Exception as ep:
                                _append(op_id, "auth0_password_fill_failed", error=str(ep) or repr(ep))
                            # Submit login
                            submitted = False
                            for sel in ['button[type="submit"]', 'button:has-text("Continue")', 'button:has-text("Log in")', 'button:has-text("Sign in")']:
                                try:
                                    page.locator(sel).first.click(timeout=8000)
                                    submitted = True
                                    _append(op_id, "auth0_submit_clicked", selector=sel)
                                    break
                                except Exception:
                                    continue
                            if not submitted:
                                _append(op_id, "auth0_submit_not_found")
                        else:
                            # Classic login page
                            try:
                                page.locator('input[name="email"]').first.fill(email)
                                page.locator('input[name="password"]').first.fill(password)
                                page.locator('button[type="submit"]').first.click()
                                _append(op_id, "classic_login_submit_clicked")
                            except Exception as ce:
                                _append(op_id, "classic_login_failed", error=str(ce) or repr(ce))
                    except Exception as le:
                        _append(op_id, "login_flow_exception", error=str(le) or repr(le))
                    # Wait for app domain
                    try:
                        page.wait_for_url("https://app.quartzy.com/**", timeout=60000)
                    except Exception:
                        _append(op_id, "app_wait_timeout", current_url=page.url)
                    # Persist session to skip future logins (only when enabled)
                    if reuse_session:
                        try:
                            context.storage_state(path=storage_state_path)  # type: ignore
                            _append(op_id, "storage_state_saved", path=storage_state_path)
                        except Exception as se:
                            _append(op_id, "storage_state_save_failed", error=str(se) or repr(se))
                _snap("after_login", page, op_id, tracing_enabled)
                _dismiss_banners()
                # Quick check for visible login error message
                try:
                    err_sel = page.locator('text="Invalid"').first
                    if err_sel and err_sel.count() > 0 and err_sel.is_visible():
                        _append(op_id, "login_error_visible")
                except Exception:
                    pass

                # Navigate to order page
                page.goto(str(app_url), wait_until="domcontentloaded")
                _append(op_id, "goto_order", url=page.url)
                _snap("order_loaded", page, op_id, tracing_enabled)
                _dismiss_banners()

                # Enter edit mode (page may already be editable; that's fine)
                edit_clicked = _click_first([
                    'button:has-text("Edit")',
                    'role=button[name="Edit"]',
                    'text=Edit',
                    'button[aria-label="Edit"]',
                    'button:has-text("Edit Order")',
                    'button:has-text("Edit Request")',
                ], timeout=6000, name="click_edit")
                if not edit_clicked:
                    _append(op_id, "edit_button_not_found")

                # If quantity already equals target, short-circuit success
                try:
                    pre_el = _find_quantity_input()
                    if pre_el is not None:
                        try:
                            pre_val = pre_el.input_value()
                        except Exception:
                            pre_val = None
                        if pre_val is not None:
                            _append(op_id, "quantity_input_value", value=pre_val)
                            try:
                                if str(int(pre_val)) == str(int(target_quantity)):
                                    _set_job(op_id, status="adjusted", success=True, order_id=order_id, new_expected=int(target_quantity), app_url=app_url, verified_value=str(pre_val), already_correct=True)
                                    _append(op_id, "adjust_already_correct", value=str(pre_val))
                                    return
                            except Exception:
                                pass
                except Exception:
                    pass

                # Fill quantity expected to target
                qty_set = False
                try:
                    qty_el = _find_quantity_input()
                    if qty_el is not None:
                        qty_set = _fill_input_robust(qty_el, int(target_quantity))
                        if qty_set:
                            _append(op_id, "quantity_filled", value=int(target_quantity))
                    else:
                        _append(op_id, "quantity_input_not_found")
                except Exception as e:
                    _append(op_id, "quantity_locate_failed", error=str(e) or repr(e))
                _snap("after_fill", page, op_id)

                # Save (some pages auto-save; if save button not present we will verify directly)
                saved = _click_first([
                    'button:has-text("Save")',
                    'role=button[name="Save"]',
                    'button:has-text("Update")',
                    'button[aria-label="Save"]',
                    'button:has-text("Done")',
                ], timeout=6000, name="click_save")
                if not saved:
                    _append(op_id, "save_click_not_found")
                    try:
                        # Nudge blur to trigger autosave if applicable
                        page.keyboard.press('Tab')
                        page.wait_for_timeout(500)
                    except Exception:
                        pass

                try:
                    # Avoid long waits; domcontentloaded is enough
                    page.wait_for_load_state("domcontentloaded")
                except Exception:
                    pass
                try:
                    page.wait_for_timeout(1500)
                except Exception:
                    pass
                _snap("after_save", page, op_id)

                # Verify
                verified_match = False
                verified_val: Optional[str] = None
                try:
                    # First check without reload
                    ver_el1 = _find_quantity_input()
                    if ver_el1 is not None:
                        try:
                            verified_val = ver_el1.input_value()
                            if verified_val is not None and str(int(verified_val)) == str(int(target_quantity)):
                                verified_match = True
                        except Exception:
                            pass
                    # If not matched, reload lightly and re-check
                    if not verified_match:
                        page.goto(str(app_url), wait_until="domcontentloaded")
                        ver_el = _find_quantity_input()
                        if ver_el is not None:
                            try:
                                verified_val = ver_el.input_value()
                            except Exception:
                                verified_val = None
                            _append(op_id, "verify_quantity_value", value=verified_val)
                            try:
                                if verified_val is not None and str(int(verified_val)) == str(int(target_quantity)):
                                    verified_match = True
                            except Exception:
                                verified_match = False
                except Exception as ve:
                    _append(op_id, "verify_exception", error=str(ve) or repr(ve))

                # Determine final outcome strictly by verification
                if verified_match or (qty_set and (saved or verified_match)):
                    _set_job(op_id, status="adjusted", success=True, order_id=order_id, new_expected=int(target_quantity), app_url=app_url, verified_value=verified_val)
                    _append(op_id, "adjust_done", order_id=order_id, new_expected=int(target_quantity))
                else:
                    reason = None
                    if not qty_set and not verified_match:
                        reason = (reason or "") + ("quantity_not_set ")
                    if not saved and not verified_match:
                        reason = (reason or "") + ("save_not_clicked ")
                    if not verified_match:
                        reason = (reason or "") + ("verify_mismatch ")
                    _set_job(op_id, status="error", success=False, error=(reason or "adjust_failed").strip(), order_id=order_id, expected=int(target_quantity), verified_value=verified_val, app_url=app_url)
                    _append(op_id, "adjust_failed", error=reason or "verify_mismatch", expected=int(target_quantity), verified=verified_val)
            finally:
                # Screenshot
                try:
                    os.makedirs("uploads", exist_ok=True)
                    shot = os.path.join("uploads", f"order_adjust_{op_id}_final.png")
                    try:
                        page.screenshot(path=shot, full_page=True)
                        _append(op_id, "screenshot_saved", path=shot)
                    except Exception as se:
                        _append(op_id, "screenshot_failed", error=str(se))
                except Exception:
                    pass
                # Stop tracing and save
                if tracing_enabled:
                    try:
                        trace_path = os.path.join("uploads", f"order_adjust_{op_id}_trace.zip")
                        context.tracing.stop(path=trace_path)  # type: ignore
                        _append(op_id, "trace_saved", path=trace_path)
                        _set_job(op_id, trace_path=trace_path)
                    except Exception as te:
                        _append(op_id, "trace_save_failed", error=str(te) or repr(te))
                try:
                    browser.close()
                except Exception:
                    pass
    except Exception as e:
        err_str = str(e) or repr(e)
        tb = traceback.format_exc()
        _set_job(op_id, status="error", success=False, error=err_str, traceback=tb)
        _append(op_id, "job_exception", error=err_str, traceback=tb)

__all__ = ["start_adjust_order_quantity", "get_job"]
