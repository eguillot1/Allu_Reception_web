"""Power Automate posting service (Phase 2c extraction).
Matches logic from server_3.py post_to_power_automate_structured + wrapper.
"""
from __future__ import annotations
import os
import json
import base64
import time
from typing import List, Optional, Tuple
import requests
from app.config import CONFIG as _APP_CONFIG

POWER_AUTOMATE_WEBHOOK_URL = _APP_CONFIG.power_automate.webhook_url

def post_to_power_automate_structured(
    *,
    item_name: str,
    supplier: str,
    quantity: str = "",
    item_type: str = "",
    project_manager: str = "",
    bsl2: str = "",
    status: str = "",
    storage_location: str = "",
    location: str = "",
    sub_location: str = "",
    received_by: str = "",
    received_by_id: str = "",
    client: str = "",
    comments: str = "",
    file_paths: Optional[List[str]] = None,
    links_only: bool = False,
    timeout: int = 120,
) -> Tuple[bool,str,dict]:
    if not POWER_AUTOMATE_WEBHOOK_URL:
        msg = "Webhook URL missing"
        return False, msg, {
            "item_name": item_name or "",
            "supplier": supplier or "",
            "quantity": str(quantity or ""),
            "links_only": bool(links_only),
            "files_count": len(file_paths or []),
            "filenames": [os.path.basename(p) for p in (file_paths or [])],
            "early_exit": True,
            "reason": "missing_webhook"
        }
    files_payload = []
    total_encoded_bytes = 0
    for p in file_paths or []:
        if not p:
            continue
        full = p if os.path.isabs(p) else os.path.join(os.getcwd(), p)
        if not os.path.exists(full):
            alt = os.path.abspath(p)
            if alt != full and os.path.exists(alt):
                full = alt
            else:
                continue
        try:
            with open(full, "rb") as f:
                raw_bytes = f.read()
                b64 = base64.b64encode(raw_bytes).decode("ascii")
            total_encoded_bytes += len(b64)
            files_payload.append({"filename": os.path.basename(full), "content_base64": b64})
        except Exception:
            pass
    try:
        max_inline = int(os.getenv("PA_MAX_INLINE_BYTES", "750000"))
    except Exception:
        max_inline = 750000
    auto_switched = False
    if not links_only and total_encoded_bytes > max_inline:
        auto_switched = True
        links_only = True
        files_payload = [{"filename": f.get("filename"), "note": "omitted (links_only)"} for f in files_payload]
    payload = {
        "item_name": item_name or "",
        "supplier": supplier or "",
        "quantity": str(quantity or ""),
        "item_type": item_type or "",
        "project_manager": project_manager or "",
        "status": status or "",
        "storage_location": storage_location or "",
        "location": location or "",
        "sub_location": sub_location or "",
        "received_by": received_by or "",
        "received_by_id": received_by_id or "",
        "client": client or "",
        "comments": comments or "",
        "links_only": bool(links_only),
        "files": files_payload,
        "_meta": {
            "auto_switched": auto_switched,
            "total_encoded_bytes": total_encoded_bytes,
            "max_inline_threshold": max_inline,
        }
    }
    debug_payload = {
        "item_name": payload["item_name"],
        "project_manager": payload["project_manager"],
        "supplier": payload["supplier"],
        "quantity": payload["quantity"],
        "item_type": payload["item_type"],
        "status": payload["status"],
    "client": payload["client"],
        "links_only": payload["links_only"],
        "files_count": len(files_payload),
        "filenames": [f.get("filename") for f in files_payload],
        "total_encoded_bytes": total_encoded_bytes,
        "max_inline_threshold": max_inline,
        "auto_switched": auto_switched,
        "_note": "BSL2 excluded from PA payload per user request"
    }
    try:
        transient_codes = {429, 502, 503, 504}
        attempt = 0
        last_err = None
        while attempt < 3:
            attempt += 1
            try:
                resp = requests.post(POWER_AUTOMATE_WEBHOOK_URL, json=payload, timeout=timeout)
                if resp.status_code in transient_codes:
                    last_err = resp.text
                elif not resp.ok:
                    resp.raise_for_status()
                else:
                    return True, ("Sent to Power Automate" + (" (links_only)" if links_only else "")), debug_payload
            except Exception as req_e:  # noqa: F841
                last_err = str(req_e)
            if attempt < 3:
                time.sleep(2 ** (attempt - 1))
        return False, (f"Failed after retries: {last_err}" if last_err else "Failed after retries"), debug_payload
    except Exception as e:  # noqa: F841
        return False, str(e), debug_payload


def post_to_power_automate(data: dict):
    pictures_raw = data.get("pictures") or ""
    file_paths = [p.strip() for p in str(pictures_raw).split(",") if p.strip()]
    return post_to_power_automate_structured(
        item_name=data.get("item_name", ""),
        supplier=data.get("supplier", ""),
        quantity=str(data.get("quantity", "") or ""),
        item_type=data.get("item_type", ""),
        project_manager=data.get("project_manager", ""),
        bsl2=data.get("bsl2_status", "") or data.get("bsl2", ""),
        status=data.get("status", ""),
        storage_location=data.get("storage_location", ""),
        location=data.get("location", ""),
        sub_location=data.get("sub_location", ""),
        received_by=data.get("received_by", ""),
        received_by_id=data.get("received_by_id", ""),
        client=data.get("client", ""),
        comments=data.get("comments", ""),
        file_paths=file_paths,
        links_only=bool(data.get("links_only", False)),
    )

__all__ = ["post_to_power_automate_structured","post_to_power_automate"]
