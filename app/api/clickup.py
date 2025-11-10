"""ClickUp / Samples related API router.

Import the service module (not individual functions) so tests can monkeypatch
symbols on the module and have effects reflected here.
"""
from fastapi import APIRouter, Query
import app.services.clickup_service as clickup_service

router = APIRouter(prefix="/api", tags=["clickup"])

@router.get("/samples_tasks")
def samples_tasks():
    return clickup_service.get_samples_tasks()

@router.get("/samples_tasks/{task_id}")
def sample_task(task_id: str):
    return clickup_service.get_sample_task(task_id)

@router.get("/employees")
def employees():
    return clickup_service.get_employees()

@router.get("/storage_locations")
def storage_locations():
    from app.services.storage_locations_service import STORAGE_LOCATIONS
    return {"storage_locations": STORAGE_LOCATIONS}

@router.get("/clickup/health")
def clickup_health(probe: int = Query(default=0, ge=0, le=1)):
    """Return ClickUp configuration health.
    probe=1 attempts a lightweight fetch using existing service function (may hit ClickUp).
    """
    has_token = bool(getattr(clickup_service, "CLICKUP_API_TOKEN", None))
    team_id = getattr(clickup_service, "TEAM_ID", None)
    list_id = getattr(clickup_service, "LIST_ID", None)
    received_status_value = getattr(clickup_service, "FORCED_STATUS_VALUE", "")
    parallel = bool(getattr(clickup_service, "ENABLE_PARALLEL_UPDATES", False))
    enabled = bool(getattr(clickup_service, "CLICKUP_ENABLED", True))
    info = {
        "configured": has_token and bool(team_id) and bool(list_id),
        "has_token": has_token,
        "team_id": str(team_id or ""),
        "list_id": str(list_id or ""),
        "received_status_value": received_status_value,
        "parallel_updates": parallel,
        "enabled": enabled,
    }
    if probe:
        try:
            res = clickup_service.get_samples_tasks()
            info["probe_result"] = {k: (len(v) if isinstance(v, list) else v) for k, v in res.items() if k in ("filtered_tasks","error")}
        except Exception as e:
            info["probe_error"] = str(e)
    return info

@router.post("/clickup/update")
def clickup_update(payload: dict):
    """Direct ClickUp update passthrough for testing.
    Requires payload.task_id and accepts the same keys used in submission mapping.
    Returns the status report from send_to_clickup without additional wrapping.
    """
    task_id = (payload or {}).get("task_id")
    if not task_id:
        return {"success": False, "reason": "no_task_id"}
    return clickup_service.send_to_clickup(payload)
