"""Pydantic response models for API endpoints."""
from pydantic import BaseModel, Field
from typing import Optional, List, Any

class PowerAutomateDebug(BaseModel):
    item_name: str
    project_manager: str | None = None
    supplier: str | None = None
    quantity: str | None = None
    item_type: str | None = None
    status: str | None = None
    links_only: bool
    files_count: int
    filenames: List[str]
    total_encoded_bytes: int
    max_inline_threshold: int
    auto_switched: bool
    _note: str | None = None

class SubmitResult(BaseModel):
    message: str = "Reception data logged successfully"
    power_automate: Any
    clickup: Any
    quartzy: Any | None = None
    quartzy_inventory: Any | None = None
    overall_success: bool

class OrderItem(BaseModel):
    id: str
    name: str
    vendor: str | None = None
    quantity_expected: int | None = None
    status: str | None = None

class OrdersResponse(BaseModel):
    orders: List[OrderItem]
    cached: bool | None = None
    meta: dict | None = None
    debug: list | None = None

class InventoryLookupResponse(BaseModel):
    found: bool
    code: str
    reason: str | None = None
    page: int | None = None
    pages_scanned: int | None = None
    item: Any | None = None
    normalized: Any | None = None
    form_mapping: Any | None = None
    suggest_additional_fields: List[str] | None = None

__all__ = [
    "SubmitResult","OrdersResponse","OrderItem","InventoryLookupResponse","PowerAutomateDebug"
]
