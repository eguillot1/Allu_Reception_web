"""ClickUp service extraction (Phase 2b).
Behavior replicated from server_3.py (field mapping, updates, attachments, metadata extraction).
No functional changes intended.
"""
from __future__ import annotations
import os
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple
import requests
from app.config import CONFIG as _APP_CONFIG
from app.services.storage_locations_service import STORAGE_DROPDOWN_IDS as _STORAGE_DROPDOWN_IDS
import threading

# Centralized configuration (Phase 5 consolidation)
CLICKUP_API_TOKEN = _APP_CONFIG.clickup.api_token
TEAM_ID = _APP_CONFIG.clickup.team_id
LIST_ID = _APP_CONFIG.clickup.list_id
PROJECT_MANAGER_CF_ID = _APP_CONFIG.clickup.cf_project_manager_id or ""
BSL2_CHECKBOX_CF_ID = _APP_CONFIG.clickup.cf_bsl2_checkbox_id or "f394f3db-0812-4ecf-91d8-2ea9a608762e"
FORCED_STATUS_VALUE = (_APP_CONFIG.clickup.received_status_value or "11e8e2f0-c21a-4459-879d-e47fc09a36db").strip()
ENABLE_PARALLEL_UPDATES = bool(_APP_CONFIG.clickup.parallel_updates)
PARALLEL_MAX_WORKERS = max(1, int(_APP_CONFIG.clickup.parallel_max_workers or 5))
CLICKUP_ENABLED = os.getenv("CLICKUP_ENABLED", "1").strip().lower() not in {"0","false","no"}

# Use centralized mapping from storage_locations_service
STORAGE_DROPDOWN_IDS = _STORAGE_DROPDOWN_IDS

form_to_cf_ids = {
    "comments": "4e732f3f-04dc-4f7f-9ffa-07317f769ba8",
    "location": "5373d5de-7ea2-4374-9c23-aff658c0e0f0",
    "received_by_id": "939760e2-1945-4570-b244-264e66bf9004",
    "status": "8a0f12f6-9827-4d57-a2f6-0fe65160b3be",
    "storage_location": "05d2d9ce-9a51-45d9-9af7-1d4218d229de",
    "sub_location": "e24983e9-71f7-44e5-b0fd-aabd391aa004",
    "item_number": "e34a130b-733b-477f-a8de-3e8765831c44",
    "clickup_received_date": "b4eca93f-a22a-47de-8937-267332673412",
}

MAPPING_SPEC = [
    ("comments", form_to_cf_ids["comments"], "comments"),
    ("location", form_to_cf_ids["location"], "location"),
    ("received_by_id", form_to_cf_ids["received_by_id"], "extracted numeric from received_by / received_by_id"),
    ("status", form_to_cf_ids["status"], f"forced option id {FORCED_STATUS_VALUE or '11e8e2f0-c21a-4459-879d-e47fc09a36db'}"),
    ("storage_location", form_to_cf_ids["storage_location"], "storage_location label (mapped to ID)"),
    ("sub_location", form_to_cf_ids["sub_location"], "sub-location or sub_location"),
    ("item_number", form_to_cf_ids["item_number"], "quantity (# samples received)"),
    ("clickup_received_date", form_to_cf_ids["clickup_received_date"], "timestamp (epoch ms)"),
]

# ---------------- Helper utilities ----------------

def iso_to_epoch_ms(iso_str: str):
    if not iso_str:
        return None
    try:
        if iso_str.endswith('Z'):
            iso_str = iso_str[:-1]
        dt = datetime.fromisoformat(iso_str)
        if not dt.tzinfo:
            from datetime import timezone
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None

# ---------------- Metadata extraction ----------------

def extract_task_meta(custom_fields):
    pm_val = ""
    pm_id = None
    bsl2_flag = False
    for cf in custom_fields or []:
        cid = cf.get("id")
        val = cf.get("value")
        if PROJECT_MANAGER_CF_ID and cid == PROJECT_MANAGER_CF_ID:
            if isinstance(val, dict):
                pm_id = val.get("id") or val.get("value") or pm_id
                pm_val = val.get("name") or val.get("username") or pm_val
            elif isinstance(val, list) and val:
                first = val[0]
                if isinstance(first, dict):
                    pm_id = first.get("id") or pm_id
                    pm_val = first.get("name") or first.get("username") or pm_val
            elif isinstance(val, str):
                pm_val = val
        if cid == BSL2_CHECKBOX_CF_ID:
            if isinstance(val, bool):
                bsl2_flag = val
            elif isinstance(val, (int, float)):
                bsl2_flag = val == 1
            elif isinstance(val, str):
                bsl2_flag = val.strip().lower() in {"true","1","yes","y"}
    if not pm_val:
        for cf in custom_fields or []:
            name_lower = (cf.get("name") or "").lower()
            if "project" in name_lower and "manager" in name_lower:
                val = cf.get("value")
                if isinstance(val, str) and not pm_val:
                    pm_val = val
                elif isinstance(val, dict):
                    pm_id = pm_id or val.get("id") or val.get("value")
                    pm_val = pm_val or val.get("name") or val.get("username") or val.get("value")
                elif isinstance(val, list) and val:
                    first = val[0]
                    if isinstance(first, dict):
                        pm_id = pm_id or first.get("id")
                        pm_val = pm_val or first.get("name") or first.get("username")
                break
    return pm_val, bsl2_flag, pm_id

# ---------------- Core field update helpers ----------------

def clickup_set_custom_field(task_id: str, field_id: str, value):
    if not CLICKUP_API_TOKEN:
        return False
    try:
        headers = {"Authorization": CLICKUP_API_TOKEN, "Content-Type": "application/json"}
        url = f"https://api.clickup.com/api/v2/task/{task_id}/field/{field_id}"
        payload = {"value": value} if not (isinstance(value, dict) and ("add" in value or "remove" in value)) else {"value": value}
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if not resp.ok:
            print(f"[ClickUp][Field] {field_id} failed {resp.status_code}: {resp.text[:300]} | payload={payload}")
            return False
        print(f"[ClickUp][Field] Updated {field_id} payload={payload}")
        return True
    except Exception as e:
        print(f"[ClickUp][Field] Exception updating {field_id}: {e}")
        return False

# ---------------- Public task update entry ----------------

def send_to_clickup(data: Dict[str, Any]):
    if not CLICKUP_ENABLED:
        print("[ClickUp] Disabled via CLICKUP_ENABLED; skipping task update.")
        return {"success": False, "reason": "disabled", "skipped": True}
    if not CLICKUP_API_TOKEN:
        print("[ClickUp] API token not configured; skipping task update.")
        return {"success": False, "reason": "no_token"}
    task_id = data.get("task_id")
    if not task_id:
        print("[ClickUp] No task_id provided; skipping task update.")
        return {"success": False, "reason": "no_task_id"}
    status_report = {
        "task_id": task_id,
        "task_url": f"https://app.clickup.com/t/{task_id}",
        "bulk_attempted": False,
        "bulk_success": False,
        "fallback_used": False,
        "mode": "per_field_only",
        "failed_fields": [],
        "failed_fields_named": [],
        "attachment_attempted": False,
        "success": False,
        "bulk_payload_preview": [],
        "field_update_plan": [],
        "forced_status_value": FORCED_STATUS_VALUE,
        "storage_location_mapping": {},
        "field_errors": {},
        "skipped_fields": [],
        "skipped_fields_named": [],
        "parallel_used": False,
        "no_changes": False,
        "mapping_spec": MAPPING_SPEC,
    }
    try:
        headers = {"Authorization": CLICKUP_API_TOKEN, "Content-Type": "application/json"}
        received_iso = data.get("timestamp", "")
        received_epoch = iso_to_epoch_ms(received_iso)
        existing_cf_map = {}
        try:
            task_resp = requests.get(
                f"https://api.clickup.com/api/v2/task/{task_id}",
                headers=headers,
                timeout=25,
            )
            if task_resp.ok:
                task_json = task_resp.json()
                for cf in task_json.get("custom_fields", []) or []:
                    existing_cf_map[cf.get("id")] = cf.get("value")
            else:
                status_report["existing_fetch_warning"] = f"HTTP {task_resp.status_code} getting task"
        except Exception as e_fetch:  # noqa: F841
            status_report["existing_fetch_warning"] = f"exception: {e_fetch}"
        base_customs = []
        comments_val = data.get("comments")
        if comments_val not in (None, ""):
            base_customs.append({"id": form_to_cf_ids["comments"], "value": comments_val, "form_key": "comments"})
        raw_storage = data.get("storage_location")
        raw_location = data.get("location")
        raw_sub_input = data.get("sub-location") or data.get("sub_location")
        status_report["raw_form"] = {
            "storage_location": raw_storage,
            "location": raw_location,
            "sub_location_input": raw_sub_input,
            "received_by": data.get("received_by"),
            "received_by_id": data.get("received_by_id"),
        }
        anomalies = []
        location_val = raw_location
        if location_val not in (None, ""):
            base_customs.append({"id": form_to_cf_ids["location"], "value": location_val, "form_key": "location"})
        received_by_raw = data.get("received_by_id") or data.get("received_by")
        if received_by_raw not in (None, ""):
            digits = "".join(ch for ch in str(received_by_raw) if ch.isdigit())
            if digits:
                try:
                    digits_int = int(digits)
                except Exception:
                    digits_int = digits
                base_customs.append({"id": form_to_cf_ids["received_by_id"], "value": {"add": [digits_int]}, "form_key": "received_by_id", "_user_id": digits_int})
                status_report["received_by_payload_format"] = {"user_id": digits_int, "format": "value.add[]"}
            else:
                status_report["received_by_id_diagnostic"] = {"provided": received_by_raw, "skipped": True, "reason": "no numeric id extracted"}
        if FORCED_STATUS_VALUE:
            base_customs.append({"id": form_to_cf_ids["status"], "value": FORCED_STATUS_VALUE, "form_key": "status"})
            status_report["forced_status_value"] = FORCED_STATUS_VALUE
        storage_label = raw_storage
        if storage_label not in (None, ""):
            mapped_val = STORAGE_DROPDOWN_IDS.get(storage_label, storage_label)
            if mapped_val != storage_label:
                status_report["storage_location_mapping"] = {"label": storage_label, "sent_value": mapped_val}
            else:
                status_report["storage_location_mapping"] = {"label": storage_label, "sent_value": mapped_val, "note": "no mapping"}
            base_customs.append({"id": form_to_cf_ids["storage_location"], "value": mapped_val, "form_key": "storage_location"})
        sub_loc_val = raw_sub_input
        if sub_loc_val not in (None, ""):
            base_customs.append({"id": form_to_cf_ids["sub_location"], "value": sub_loc_val, "form_key": "sub_location"})
        if raw_storage and raw_location and raw_sub_input:
            if location_val == raw_storage and sub_loc_val == raw_location and raw_sub_input != raw_location:
                anomalies.append("storage/location/sub_location_swapped_detected")
        if anomalies:
            status_report["anomalies"] = anomalies
        qty_val = data.get("quantity")
        if qty_val not in (None, ""):
            try:
                if isinstance(qty_val, str) and qty_val.strip().isdigit():
                    qty_conv = int(qty_val.strip())
                else:
                    qty_conv = qty_val
            except Exception:
                qty_conv = qty_val
            base_customs.append({"id": form_to_cf_ids["item_number"], "value": qty_conv, "form_key": "item_number"})
        if received_epoch is not None:
            base_customs.append({"id": form_to_cf_ids["clickup_received_date"], "value": received_epoch, "form_key": "clickup_received_date"})
        dedup = []
        seen = set()
        keep_cf_ids_env = os.getenv("CLICKUP_CUSTOM_FIELDS_IDS")
        if not keep_cf_ids_env:
            keep_cf_ids = list(form_to_cf_ids.values())
        else:
            try:
                keep_cf_ids = [i.strip() for i in keep_cf_ids_env.split(",") if i.strip()]
            except Exception:
                keep_cf_ids = list(form_to_cf_ids.values())
        for entry in base_customs:
            cid = entry.get("id")
            if keep_cf_ids and cid not in keep_cf_ids:
                continue
            if cid in seen:
                continue
            dedup.append(entry)
            seen.add(cid)
        to_update = []
        skipped = []
        for cf in dedup:
            cid = cf["id"]
            new_val = cf["value"]
            old_val = existing_cf_map.get(cid, object())
            if cid == form_to_cf_ids.get("received_by_id") and isinstance(new_val, dict) and "add" in new_val:
                existing_ids = []
                if isinstance(old_val, list):
                    for entry in old_val:
                        if isinstance(entry, dict) and "id" in entry:
                            existing_ids.append(entry.get("id"))
                        elif isinstance(entry, int):
                            existing_ids.append(entry)
                new_ids = new_val.get("add") or []
                if any(nid in existing_ids for nid in new_ids):
                    skipped.append(cf)
                    continue
            if isinstance(new_val, (int, float)) and isinstance(old_val, str) and old_val.isdigit():
                try:
                    old_val_cast = int(old_val)
                except Exception:
                    old_val_cast = old_val
            else:
                old_val_cast = old_val
            if old_val_cast == new_val:
                skipped.append(cf)
            else:
                to_update.append(cf)
        if not to_update:
            status_report["no_changes"] = True
        id_to_form_key = {v: k for k, v in form_to_cf_ids.items()}
        preview_list = [
            {"id": cf.get("id"), "form_key": cf.get("form_key") or id_to_form_key.get(cf.get("id")), "value": cf.get("value"), "skipped": cf in skipped}
            for cf in dedup
        ]
        status_report["bulk_payload_preview"] = preview_list
        status_report["field_update_plan"] = preview_list
        status_report["skipped_fields"] = [cf["id"] for cf in skipped]
        status_report["skipped_fields_named"] = [((cf.get("form_key") or id_to_form_key.get(cf["id"], cf["id"]) or "").replace("_", " ").title()) or str(cf.get("id")) for cf in skipped]
        print(f"[ClickUp] Per-field update set size: {len(dedup)} | to_update={len(to_update)} skipped={len(skipped)}")
        field_failures = []
        if to_update:
            if ENABLE_PARALLEL_UPDATES and len(to_update) > 1:
                status_report["parallel_used"] = True
                with ThreadPoolExecutor(max_workers=PARALLEL_MAX_WORKERS) as executor:
                    future_map = {executor.submit(clickup_set_custom_field, task_id, cf.get("id"), cf.get("value")): cf for cf in to_update}
                    for fut in as_completed(future_map):
                        cf = future_map[fut]
                        try:
                            ok = fut.result()
                        except Exception as e_thr:  # noqa: F841
                            ok = False
                            status_report["field_errors"][cf.get("id")] = f"exception: {e_thr}"
                        if not ok:
                            field_failures.append(cf.get("id"))
                            if cf.get("id") not in status_report["field_errors"]:
                                status_report["field_errors"][cf.get("id")] = "update_failed"
            else:
                for cf in to_update:
                    cid = cf.get("id")
                    val = cf.get("value")
                    ok = clickup_set_custom_field(task_id, cid, val)
                    if not ok:
                        field_failures.append(cid)
                        status_report["field_errors"][cid] = "update_failed"
        if field_failures:
            status_report["failed_fields"] = field_failures
            status_report["failed_fields_named"] = [(id_to_form_key.get(fid) or fid).replace("_", " ").title() for fid in field_failures]
        else:
            status_report["success"] = True
        new_comments = (data.get("comments") or "").strip()
        if new_comments:
            try:
                headers_desc = {"Authorization": CLICKUP_API_TOKEN, "Content-Type": "application/json"}
                task_detail = requests.get(f"https://api.clickup.com/api/v2/task/{task_id}", headers=headers_desc, timeout=25)
                if task_detail.ok:
                    current_desc = task_detail.json().get("description") or ""
                    line = f"\n\n---\nReception Note ({datetime.utcnow().isoformat()}Z):\n{new_comments}\n"
                    if line.strip() not in current_desc:
                        new_desc = (current_desc or "") + line
                        upd = requests.put(
                            f"https://api.clickup.com/api/v2/task/{task_id}",
                            headers=headers_desc,
                            json={"description": new_desc},
                            timeout=25,
                        )
                        if upd.ok:
                            status_report["description_appended"] = True
                        else:
                            status_report["description_appended"] = False
                            status_report["description_error"] = f"HTTP {upd.status_code}"
                else:
                    status_report["description_appended"] = False
                    status_report["description_error"] = f"fetch_http_{task_detail.status_code}"
            except Exception as e_desc:  # noqa: F841
                status_report["description_appended"] = False
                status_report["description_exception"] = str(e_desc)
        return status_report
    except Exception as e:  # noqa: F841
        print(f"[ClickUp] Failed: {e}")
        status_report["exception"] = str(e)
        return status_report

# ---------------- Attachment upload ----------------

def upload_attachments_to_task(task_id: str, pictures: str | None):
    if not pictures:
        return
    try:
        for pic in pictures.split(","):
            pic = pic.strip()
            if not pic:
                continue
            if os.path.exists(pic):
                with open(pic, "rb") as f:
                    files = {"attachment": (os.path.basename(pic), f)}
                    headers = {"Authorization": CLICKUP_API_TOKEN}
                    resp = requests.post(f"https://api.clickup.com/api/v2/task/{task_id}/attachment", headers=headers, files=files)
                    resp.raise_for_status()
                    print(f"[ClickUp] Uploaded {pic} to task {task_id}")
    except Exception as e:
        print(f"[ClickUp] Attachment upload failed: {e}")

# ---------------- Task retrieval ----------------

clickup_cache = {"data": None, "timestamp": 0}
clickup_cache_lock = threading.Lock()
CACHE_TTL = 180

raw_filter_values = os.getenv("CLICKUP_STATUS_FILTER_VALUES", "")  # still env-driven (intentionally) for dynamic filtering without restart
CLICKUP_STATUS_FILTER_VALUES = [v.strip() for v in raw_filter_values.split(",") if v.strip()]

def get_sample_task(task_id: str):
    if not CLICKUP_API_TOKEN or not LIST_ID:
        return {"error": "Missing ClickUp credentials."}
    try:
        headers = {"Authorization": CLICKUP_API_TOKEN}
        resp = requests.get(
            f"https://api.clickup.com/api/v2/task/{task_id}",
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        t = resp.json()
        cfields = t.get("custom_fields", [])
        pm_val, bsl2_val, pm_id = extract_task_meta(cfields)
        return {
            "id": t.get("id"),
            "name": t.get("name"),
            "custom_fields": cfields,
            "description": t.get("description", ""),
            "project_manager": pm_val,
            "project_manager_id": pm_id,
            "bsl2_status": bsl2_val,
        }
    except Exception as e:  # noqa: F841
        return {"error": str(e)}


def get_samples_tasks():
    if not CLICKUP_API_TOKEN or not LIST_ID:
        return {"error": "Missing ClickUp credentials."}
    try:
        from time import time as _time
        now = _time()
        with clickup_cache_lock:
            if clickup_cache["data"] and now - clickup_cache["timestamp"] < CACHE_TTL:
                return clickup_cache["data"]
        headers = {"Authorization": CLICKUP_API_TOKEN}
        filters = [{
            "field_id": form_to_cf_ids["status"],
            "operator": "ANY",
            "value": CLICKUP_STATUS_FILTER_VALUES,
        }]
        params = {"custom_fields": json.dumps(filters)}
        resp = requests.get(
            f"https://api.clickup.com/api/v2/list/{LIST_ID}/task",
            headers=headers,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        tasks = resp.json().get("tasks", [])
        unique_tasks = {}
        for t in tasks:
            tid = t.get("id")
            if not tid:
                continue
            cfields = t.get("custom_fields", []) or []
            pm_val, bsl2_val, pm_id = extract_task_meta(cfields)
            unique_tasks[tid] = {
                "id": tid,
                "name": t.get("name"),
                "custom_fields": cfields,
                "description": t.get("description", ""),
                "project_manager": pm_val,
                "project_manager_id": pm_id,
                "bsl2_status": bsl2_val,
            }
        result = {"filtered_tasks": list(unique_tasks.values())}
        with clickup_cache_lock:
            clickup_cache["data"] = result
            clickup_cache["timestamp"] = now
        return result
    except Exception as e:  # noqa: F841
        return {"error": str(e)}


def get_employees():
    group_id = os.getenv("RECEPTION_GROUP_ID") or os.getenv("CLICKUP_RECEPTION_GROUP_ID")
    debug = {}
    error = None
    members = []
    if CLICKUP_API_TOKEN and TEAM_ID and group_id:
        try:
            headers = {"Authorization": CLICKUP_API_TOKEN, "accept": "application/json"}
            url = f"https://api.clickup.com/api/v2/group?team_id={TEAM_ID}&group_ids={group_id}"
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            groups = data.get("groups", [])
            group_obj = None
            for g in groups:
                if str(g.get("id")) == str(group_id):
                    group_obj = g
                    break
            debug["group_obj"] = group_obj
            if group_obj:
                for m in group_obj.get("members", []):
                    user = m.get("user", {}) if "user" in m else m
                    members.append({
                        "id": user.get("id"),
                        "username": user.get("username"),
                        "email": user.get("email"),
                        "name": user.get("name") or user.get("username") or user.get("email"),
                    })
            debug["parsed_members"] = members
        except Exception as e:  # noqa: F841
            error = f"[ClickUp] Error fetching Reception Group members: {e}"
            debug["clickup_error"] = error
    if not members:
        reception_users = os.getenv("RECEPTION_USERS")
        if reception_users:
            try:
                members = json.loads(reception_users)
                debug["static_members"] = members
            except Exception as e:  # noqa: F841
                error = f"[Reception Users] Failed to parse RECEPTION_USERS: {e}"
                debug["static_error"] = error
    return {"members": members, "debug": debug, "error": error}

__all__ = [
    "send_to_clickup","clickup_set_custom_field","extract_task_meta","upload_attachments_to_task",
    "form_to_cf_ids","MAPPING_SPEC","STORAGE_DROPDOWN_IDS","FORCED_STATUS_VALUE",
    "get_sample_task","get_samples_tasks","get_employees",
]
