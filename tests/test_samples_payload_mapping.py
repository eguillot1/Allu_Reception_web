import json
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_samples_payload_client_and_item_fields(monkeypatch):
    captured = {}

    def fake_pa(**kwargs):
        # Capture the structured kwargs sent to Power Automate
        captured.update(kwargs)
        return True, "Sent (mock)", {"mock": True, "client": kwargs.get("client"), "supplier": kwargs.get("supplier"), "item_type": kwargs.get("item_type"), "item_name": kwargs.get("item_name")}

    # Mock ClickUp update since samples flow will call it
    def fake_click(data):
        return {"success": True, "task_id": data.get("task_id"), "mock": True}

    monkeypatch.setattr("app.services.power_automate_service.post_to_power_automate_structured", fake_pa)
    monkeypatch.setattr("app.services.clickup_service.send_to_clickup", fake_click)

    payload = {
        "form_type": "samples",
        "task_id": "S-123",
        "quantity": "3",
        "client": "ACME Biotech",
        "package_status": "All Good",
        # item_name intentionally omitted; frontend sets it to task name now, but backend should accept missing gracefully
        # supplier intentionally omitted for samples flow
        "item_type": "Samples",
    }

    r = client.post("/api/submit", json=payload)
    assert r.status_code == 200, r.text

    # Validate Power Automate mapping: client should be ACME, supplier should fallback to client per rule
    assert captured.get("client") == "ACME Biotech"
    assert captured.get("supplier") == "ACME Biotech"
    # item_type could be 'Samples' (frontend) or empty if frontend not involved; allow both but prefer 'Samples'
    # Here we passed 'Samples' explicitly in payload, so it should be 'Samples'
    assert captured.get("item_type") == "Samples"
