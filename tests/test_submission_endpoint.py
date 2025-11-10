import json
from fastapi.testclient import TestClient

# Import the transitional app
from app.main import app

client = TestClient(app)


def test_submit_samples_minimal(monkeypatch):
    # Monkeypatch external service calls to avoid real HTTP
    from app.services.power_automate_service import post_to_power_automate_structured
    from app.services.clickup_service import send_to_clickup

    def fake_pa(**kwargs):
        return True, "Sent (mock)", {"mock": True}

    def fake_click(data):
        return {"success": True, "task_id": data.get("task_id"), "mock": True}

    monkeypatch.setattr("app.services.power_automate_service.post_to_power_automate_structured", fake_pa)
    monkeypatch.setattr("app.services.clickup_service.send_to_clickup", fake_click)

    payload = {
        "form_type": "samples",
        "task_id": "12345",
        "item_name": "Test Sample",
        "quantity": "10"
    }
    r = client.post("/api/submit", json=payload)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["overall_success"] is True
    assert data["clickup"]["success"] is True
    assert data["power_automate"][0] is True  # tuple (bool,msg,debug)


def test_submit_other_with_quartzy(monkeypatch):
    from app.services.power_automate_service import post_to_power_automate_structured
    from app.services.quartzy_service import quartzy_update_order_status, quartzy_inventory_lookup

    def fake_pa(**kwargs):
        return True, "Sent (mock)", {"mock": True}

    def fake_quartzy_update(order_id):
        return {"success": True, "order_id": order_id, "mock": True}

    def fake_inventory_lookup(code, max_pages=None):
        return {"found": False, "code": code, "mock": True}

    monkeypatch.setattr("app.services.power_automate_service.post_to_power_automate_structured", fake_pa)
    monkeypatch.setattr("app.services.quartzy_service.quartzy_update_order_status", fake_quartzy_update)
    monkeypatch.setattr("app.services.quartzy_service.quartzy_inventory_lookup", fake_inventory_lookup)

    payload = {
        "form_type": "other",
        "order_id": "QO-123",
        "item_name": "Buffer",
        "quantity": "2"
    }
    r = client.post("/api/submit", json=payload)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["overall_success"] is True
    assert data["quartzy"]["success"] is True
