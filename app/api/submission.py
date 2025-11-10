"""Submission & upload router extracted from legacy server_3 endpoints.
This router preserves existing behavior while adding Pydantic validation.
"""
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Body, Request
from typing import List, Optional, Any, Union
import os

# Attempt to import service-layer utilities from legacy or new modules
try:
    import app.services.clickup_service as clickup_service
    import app.services.power_automate_service as pa_service
    import app.services.quartzy_service as quartzy_service
    from app.storage.logging_store import log_reception_to_db, ensure_db, ensure_csv_with_header
except Exception:
    # Fallback dynamic imports if running inside legacy context
    import services.clickup_service as clickup_service  # type: ignore
    import services.power_automate_service as pa_service  # type: ignore
    import services.quartzy_service as quartzy_service  # type: ignore
    from storage.logging_store import log_reception_to_db, ensure_db, ensure_csv_with_header  # type: ignore

from app.models.requests import SubmitRequest
from app.models.responses import SubmitResult
from app.services.storage_locations_service import STORAGE_DROPDOWN_IDS

router = APIRouter(prefix="/api", tags=["submission"])

LOG_CSV = "reception_log.csv"
try:
    ensure_csv_with_header()
    ensure_db()
except Exception:
    pass

@router.post("/submit", response_model=SubmitResult)
async def submit_reception(
    request: Request,
    # JSON body (preferred new contract)
    payload: Optional[SubmitRequest] = Body(None),
    # Legacy form fields (all optional; used when JSON body not provided)
    form_type: Optional[str] = Form(None),
    task_id: Optional[str] = Form(None),
    order_id: Optional[str] = Form(None),
    inventory_item_id: Optional[str] = Form(None),
    item_name: Optional[str] = Form(None),
    project_manager: Optional[str] = Form(None),
    supplier: Optional[str] = Form(None),
    catalog_number: Optional[str] = Form(None),
    storage_location: Optional[str] = Form(None),
    location: Optional[str] = Form(None),
    sub_location: Optional[str] = Form(None),
    lot_number: Optional[str] = Form(None),
    quantity: Optional[str] = Form(None),
    item_type: Optional[str] = Form(None),
    bsl2_status: Optional[str] = Form(None),  # legacy checkbox string
    bsl2: Optional[bool] = Form(None),        # new boolean field (if present)
    client: Optional[str] = Form(None),
    package_status: Optional[str] = Form(None),
    received_by: Optional[str] = Form(None),
    received_by_id: Optional[str] = Form(None),
    comments: Optional[str] = Form(None),
):
    # If JSON body absent, try to read JSON explicitly (when Form params are present FastAPI may not bind Body)
    if payload is None:
        try:
            ct = (request.headers.get("content-type") or "").lower()
            if "application/json" in ct:
                raw = await request.json()
                if isinstance(raw, dict) and raw.get("form_type"):
                    payload = SubmitRequest(**raw)
        except Exception:
            payload = None
    # If still absent, synthesize SubmitRequest from legacy form fields
    if payload is None:
        # Derive bsl2 boolean from either explicit bsl2 or textual bsl2_status
        if bsl2 is not None:
            bsl2_bool = bsl2
        else:
            txt = (bsl2_status or "").strip().lower()
            bsl2_bool = txt in {"true","1","yes","y","checked"}
        try:
            # Infer form type: prefer explicit, else if task_id present assume samples, if order_id present assume other
            inferred_form_type = (form_type or ("samples" if task_id else ("other" if order_id else "samples"))).lower()
            payload = SubmitRequest(
                form_type=inferred_form_type,
                task_id=task_id,
                order_id=order_id,
                inventory_item_id=inventory_item_id,
                item_name=item_name or "Unnamed",
                project_manager=project_manager,
                supplier=supplier,
                catalog_number=catalog_number,
                storage_location=storage_location,
                location=location,
                sub_location=sub_location,
                lot_number=lot_number,
                quantity=quantity,
                item_type=item_type,
                bsl2=bsl2_bool,
                client=client,
                package_status=package_status,
                received_by=received_by,
                received_by_id=received_by_id,
                comments=comments,
            )
        except Exception as e:
            raise HTTPException(status_code=422, detail=f"Invalid form submission: {e}")
    # Core orchestration similar to legacy submit endpoint
    form_type = payload.form_type
    results: dict[str, Any] = {"power_automate": None, "clickup": None, "quartzy": None, "quartzy_inventory": None, "quartzy_inventory_upsert": None}
    success_flags = []

    # Establish a timestamp (use provided if available, else now UTC)
    from datetime import datetime as _dt
    ts_iso = payload.timestamp or _dt.utcnow().isoformat()

    # Power Automate first (always attempts) but do not block the response for long
    try:
        import asyncio
        import os as _os
        # Allow tuning via env: hard API timeout for the PA HTTP call and how long we wait before returning
        pa_http_timeout = int(_os.getenv("PA_TIMEOUT_S", "30") or 30)
        pa_wait_before_return = float(_os.getenv("PA_WAIT_BEFORE_RETURN_S", "6") or 6)

        def _call_pa_sync():
            return pa_service.post_to_power_automate_structured(
                item_name=payload.item_name,
                supplier=payload.supplier or "",
                quantity=payload.quantity or "",
                item_type=payload.item_type or "",
                project_manager=payload.project_manager or "",
                bsl2=str(bool(payload.bsl2)),
                status=(payload.package_status or ""),
                storage_location=payload.storage_location or payload.location or "",
                location=payload.location or "",
                sub_location=payload.sub_location or "",
                received_by=payload.received_by or "",
                received_by_id=payload.received_by_id or "",
                client=payload.client or "",
                comments=payload.comments or "",
                file_paths=[],
                links_only=False,
                timeout=pa_http_timeout,
            )

        # Run in background thread but wait briefly for a quick success
        pa_future = asyncio.to_thread(_call_pa_sync)
        try:
            pa_ok, pa_msg, pa_dbg = await asyncio.wait_for(pa_future, timeout=pa_wait_before_return)
            results["power_automate"] = (pa_ok, pa_msg, pa_dbg)
        except asyncio.TimeoutError:
            # Keep running in the background; respond now so UI doesn't hang
            results["power_automate"] = {"queued": True, "note": f"continuing asynchronously (waited {pa_wait_before_return}s)"}
        success_flags.append(True)
    except Exception as e:
        results["power_automate"] = {"error": str(e)}
        success_flags.append(False)

    # ClickUp updates only for samples form
    if form_type == "samples" and payload.task_id:
        try:
            # Map storage_location label to ClickUp option id if available
            storage_label = payload.storage_location or payload.location or ""
            storage_value = STORAGE_DROPDOWN_IDS.get(storage_label, storage_label)
            # Enrich comments with Client and Package Status for ClickUp visibility
            extra_lines = []
            _client_str = (payload.client or "").strip()
            if _client_str:
                extra_lines.append(f"Client: {_client_str}")
            _pkg_str = (payload.package_status or "").strip()
            if _pkg_str:
                extra_lines.append(f"Package Status: {_pkg_str}")
            joined_extra = ("\n".join(extra_lines)).strip()
            enriched_comments = (payload.comments or "")
            if joined_extra:
                enriched_comments = (enriched_comments + ("\n" if enriched_comments else "") + joined_extra).strip()

            data = {
                "task_id": payload.task_id,
                "item_name": payload.item_name,
                "quantity": payload.quantity or "",
                "comments": enriched_comments,
                "storage_location": storage_value,
                "location": payload.location or "",
                "sub_location": payload.sub_location or "",
                "received_by_id": payload.received_by_id or "",
                "received_by": payload.received_by or "",
                "timestamp": ts_iso,
            }
            clickup_resp = clickup_service.send_to_clickup(data)
            results["clickup"] = clickup_resp
            success_flags.append(True)
        except Exception as e:
            results["clickup"] = {"error": str(e)}
            success_flags.append(False)
    else:
        # Provide explicit reason so it doesn't look like a silent no-op
        skip_reason = []
        if form_type != "samples":
            skip_reason.append("form_type_not_samples")
        if not payload.task_id:
            skip_reason.append("missing_task_id")
        try:
            from app.services import clickup_service as _ck
            if hasattr(_ck, "CLICKUP_ENABLED") and not bool(getattr(_ck, "CLICKUP_ENABLED")):
                skip_reason.append("clickup_disabled")
        except Exception:
            pass
        results["clickup"] = {"skipped": True, "reason": ",".join(skip_reason) or "conditions_not_met"}

    # If we're in 'other' flow, we can perform Order Request update and Inventory update in parallel.
    # We'll kick off the inventory upsert in a background thread, then perform the order update here,
    # and finally await the inventory result.
    inv_future = None
    inv_enabled = (form_type == "other")
    if inv_enabled:
        import asyncio
        def _do_inventory_upsert():
            _qs = quartzy_service.quartzy_service
            upsert_result = None
            inv_id = None
            # Prefer explicit inventory_item_id if provided by UI; otherwise resolve best candidate
            if getattr(payload, "inventory_item_id", None):
                inv_id = payload.inventory_item_id
            else:
                best = _qs.find_inventory_match_best(item_name=payload.item_name, vendor_name=payload.supplier, catalog_number=payload.catalog_number)
                if best and best.get("found"):
                    inv_id = (best.get("item") or {}).get("id")
            if inv_id:
                # Additive quantity update: read current, add received, then update
                try:
                    received_qty = int(str(payload.quantity or "").strip() or 0)
                except Exception:
                    received_qty = 0
                current_qty = None
                try:
                    curr = _qs.get_inventory_item(inv_id)
                    if curr and curr.get("found"):
                        cq = (curr.get("item") or {}).get("quantity")
                        if cq is not None:
                            try:
                                current_qty = int(str(cq).split(".")[0])
                            except Exception:
                                pass
                except Exception:
                    pass
                new_qty = None
                if current_qty is not None and received_qty is not None:
                    new_qty = max(0, current_qty + received_qty)
                addl_meta = {"received_qty": received_qty}
                if current_qty is not None:
                    addl_meta["current_qty"] = current_qty
                    if new_qty is not None:
                        addl_meta["new_qty"] = new_qty
                upsert_result = _qs.update_inventory_item(
                    item_id=inv_id,
                    item_name=payload.item_name,
                    vendor_name=payload.supplier,
                    catalog_number=payload.catalog_number,
                    quantity=(str(new_qty) if new_qty is not None else payload.quantity),
                    quantity_delta=received_qty,
                    location=payload.location,
                    sub_location=payload.sub_location,
                )
                try:
                    upsert_result["item_url"] = _qs.build_quartzy_item_link(inv_id)
                    upsert_result["item_id"] = inv_id
                    upsert_result["_quantity_update_debug"] = addl_meta
                    upsert_result["matched"] = True
                except Exception:
                    pass
                # UI fallback (async background) only if API update failed
                try:
                    if not bool(upsert_result.get("updated")):
                        try:
                            from app.services.quartzy_auto_update_service import start_auto_update_quantity as _start_ui_update
                            ui_payload = {
                                "item_id": inv_id,
                                "target_quantity": (new_qty if new_qty is not None else received_qty),
                                "item_url": upsert_result.get("item_url"),
                            }
                            op_id = _start_ui_update(ui_payload)
                            upsert_result["ui_fallback_started"] = True
                            upsert_result["ui_fallback_kind"] = "auto_update_quantity"
                            upsert_result["ui_op_id"] = op_id
                            upsert_result["ui_status_endpoint"] = f"/api/quartzy/inventory/auto_update/status?op_id={op_id}"
                            upsert_result["ui_screenshot"] = f"uploads/auto_update_{op_id}_final.png"
                        except Exception as _e:
                            upsert_result["ui_fallback_started"] = False
                            upsert_result["ui_fallback_error"] = str(_e)
                except Exception:
                    pass
            else:
                add_link = _qs.build_quartzy_add_link(
                    item_name=payload.item_name,
                    vendor_name=payload.supplier,
                    catalog_number=payload.catalog_number,
                    location=payload.location,
                    lab_id=None,
                )
                upsert_result = {"matched": False, "manual_add_link": add_link}
            return upsert_result

        try:
            # run inventory upsert in a worker thread so we can do order update concurrently
            try:
                inv_future = asyncio.to_thread(_do_inventory_upsert)
            except AttributeError:
                # Python <3.9 fallback
                loop = asyncio.get_running_loop()
                inv_future = loop.run_in_executor(None, _do_inventory_upsert)
        except Exception:
            inv_future = None

    # Quartzy status update for 'other' form if order_id provided
    if form_type == "other" and payload.order_id:
        try:
            # Partial reception support: if enabled and received < expected, skip marking as RECEIVED
            do_status_update = True
            partial_reason = None
            try:
                if getattr(quartzy_service.quartzy_service, "enable_partial_status", False):
                    # Parse received qty
                    recv_qty = 0
                    try:
                        recv_qty = int(str(payload.quantity or "").strip() or 0)
                    except Exception:
                        recv_qty = 0
                    # Fetch order details to read expected quantity
                    ord_res = quartzy_service.quartzy_service.get_order_request(str(payload.order_id))
                    if ord_res and ord_res.get("found") and isinstance(ord_res.get("order"), dict):
                        oq = (ord_res["order"].get("quantity"))
                        expected_qty = None
                        if oq is not None:
                            try:
                                expected_qty = int(str(oq).split(".")[0])
                            except Exception:
                                expected_qty = None
                        if expected_qty is not None and recv_qty < expected_qty:
                            # partial: do not mark RECEIVED now
                            do_status_update = False
                            partial_reason = {
                                "reason": "partial_receipt",
                                "ordered_qty": expected_qty,
                                "received_qty": recv_qty,
                            }
            except Exception:
                # Fail open: if any error in partial logic, proceed with normal status update
                do_status_update = True

            if do_status_update:
                # Default new_status to RECEIVED for basic flow
                q_resp = quartzy_service.quartzy_update_order_status(payload.order_id)
                # Normalize shape to include 'success' for tests: map from 'updated' if missing
                if isinstance(q_resp, dict) and "success" not in q_resp:
                    q_resp = {**q_resp}
                    q_resp["success"] = bool(q_resp.get("updated"))
                results["quartzy"] = q_resp
                success_flags.append(True)
            else:
                # Partial: choose adjust mode (api: annotate via notes; ui: start Playwright job). Do not change status.
                # Compute remaining
                try:
                    rem = None
                    if partial_reason and "ordered_qty" in partial_reason and "received_qty" in partial_reason:
                        rem = max(0, int(partial_reason["ordered_qty"]) - int(partial_reason["received_qty"]))
                except Exception:
                    rem = None
                adjust_meta = {"partial": True, **(partial_reason or {})}
                if rem is not None:
                    adjust_meta["remaining_qty"] = rem
                    # Provide UI-facing message
                    try:
                        rcv = None
                        exp = None
                        if isinstance(partial_reason, dict):
                            rcv = partial_reason.get("received_qty")
                            exp = partial_reason.get("ordered_qty")
                        if rcv is not None and exp is not None:
                            adjust_meta["ui_message"] = f"Partial Reception {int(rcv)}/{int(exp)}"
                        else:
                            adjust_meta["ui_message"] = "Partial Reception"
                    except Exception:
                        adjust_meta["ui_message"] = "Partial Reception"
                # Always use UI automation (Playwright) to adjust expected quantity on partial reception
                try:
                    from app.services.quartzy_order_adjust_service import start_adjust_order_quantity as _start_adjust
                    if rem is not None:
                        op_id = _start_adjust({"order_id": str(payload.order_id), "target_quantity": int(rem)})
                        adjust_meta["order_adjust_started"] = True
                        adjust_meta["order_adjust_op_id"] = op_id
                        adjust_meta["order_adjust_status_endpoint"] = f"/api/quartzy/order/adjust/status?op_id={op_id}"
                        adjust_meta["order_adjust_screenshot"] = f"uploads/order_adjust_{op_id}_final.png"
                    else:
                        adjust_meta["order_adjust_started"] = False
                        adjust_meta["order_adjust_error"] = "remaining_quantity_unknown"
                except Exception as _e:
                    adjust_meta["order_adjust_started"] = False
                    adjust_meta["order_adjust_error"] = str(_e)
                # Return metadata; consider this path successful from API perspective
                results["quartzy"] = adjust_meta
                success_flags.append(True)
        except Exception as e:
            results["quartzy"] = {"error": str(e)}
            success_flags.append(False)

    # Await inventory future if started
    if inv_enabled:
        try:
            inv_res = None
            if inv_future is not None:
                inv_res = await inv_future
            else:
                inv_res = None
            results["quartzy_inventory_upsert"] = inv_res if inv_res is not None else {"skipped": True, "reason": "inv_future_not_started"}
        except Exception as e:
            results["quartzy_inventory_upsert"] = {"error": str(e)}

    # Logging (CSV + DB)
    try:
        row = [
            ts_iso,
            payload.item_name,
            payload.quantity or "",
            payload.supplier or "",
            payload.item_type or "",
            str(bool(payload.bsl2)),
            payload.package_status or "",  # status (package status for logging)
            payload.storage_location or payload.location or "",
            payload.location or "",
            payload.sub_location or "",
            payload.received_by or "",
            payload.received_by_id or "",
            payload.project_manager or "",
            payload.comments or "",
            "",  # pictures
        ]
        # CSV + DB operations
        import csv
        ensure_csv_with_header()
        with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)
        log_reception_to_db(row)
    except Exception as e:
        results["logging_error"] = str(e)

    # Compute overall success: prefer explicit flags; fallback to checking sub-result shapes
    overall_success = all(success_flags) if success_flags else False
    if not overall_success:
        pa_ok = False
        try:
            pa_val = results.get("power_automate")
            if isinstance(pa_val, (list, tuple)) and len(pa_val) > 0:
                pa_ok = bool(pa_val[0])
        except Exception:
            pa_ok = False
        ck_ok = isinstance(results.get("clickup"), dict) and bool(results["clickup"].get("success") is True)
        q_ok = isinstance(results.get("quartzy"), dict) and bool(results["quartzy"].get("success") is True)
        overall_success = pa_ok or ck_ok or q_ok

    return SubmitResult(
        power_automate=results.get("power_automate"),
        clickup=results.get("clickup"),
        quartzy=results.get("quartzy"),
        quartzy_inventory=results.get("quartzy_inventory_upsert") or results.get("quartzy_inventory"),
        overall_success=overall_success,
    )

@router.post("/upload")
async def upload_files(files: List[UploadFile] = File(...)):
    saved = []
    errors = []
    upload_dir = "uploads"
    os.makedirs(upload_dir, exist_ok=True)
    for f in files:
        fname = f.filename or "uploaded_file"
        dest = os.path.join(upload_dir, fname)
        try:
            content = await f.read()
            with open(dest, "wb") as out:
                out.write(content)
            saved.append(fname)
        except Exception as e:
            errors.append({fname: str(e)})
    return {"saved": saved, "errors": errors}
