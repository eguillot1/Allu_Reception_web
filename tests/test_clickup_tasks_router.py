from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_samples_tasks_router(monkeypatch):
    # Mock get_samples_tasks service call
    def fake_get():
        return {"filtered_tasks": [{"id": "1", "name": "Task A"}]}

    monkeypatch.setattr("app.services.clickup_service.get_samples_tasks", fake_get)

    resp = client.get("/api/samples_tasks")
    assert resp.status_code == 200
    data = resp.json()
    assert "filtered_tasks" in data
    assert data["filtered_tasks"][0]["name"] == "Task A"
