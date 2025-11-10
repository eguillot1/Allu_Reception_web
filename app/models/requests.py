"""Pydantic request models for API endpoints."""
from pydantic import BaseModel
from typing import Optional, List

class SubmitRequest(BaseModel):
    form_type: str  # 'samples' or 'other'
    task_id: Optional[str] = None  # samples form
    order_id: Optional[str] = None  # other form (Quartzy order)
    inventory_item_id: Optional[str] = None  # explicit Quartzy inventory item id (if user selected a candidate)
    item_name: str
    project_manager: Optional[str] = None
    supplier: Optional[str] = None
    catalog_number: Optional[str] = None
    storage_location: Optional[str] = None
    location: Optional[str] = None
    sub_location: Optional[str] = None
    lot_number: Optional[str] = None
    quantity: Optional[str] = None
    item_type: Optional[str] = None
    bsl2: Optional[bool] = None
    client: Optional[str] = None
    package_status: Optional[str] = None
    received_by: Optional[str] = None
    received_by_id: Optional[str] = None
    timestamp: Optional[str] = None  # ISO8601; if not provided, backend will set current UTC
    comments: Optional[str] = None

__all__ = ["SubmitRequest"]
