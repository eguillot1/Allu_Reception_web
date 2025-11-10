"""Centralized configuration loader.

Phase 1: Read environment variables and expose a Config object.
This does NOT change existing behavior in server_3.py yet; the monolith will
later be refactored to import from here gradually.
"""
from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class ClickUpConfig:
    api_token: str | None
    team_id: str | None
    list_id: str | None
    reception_group_id: str | None
    received_status_value: str | None
    parallel_updates: bool
    parallel_max_workers: int
    cf_project_manager_id: str | None
    cf_bsl2_checkbox_id: str | None

@dataclass(frozen=True)
class PowerAutomateConfig:
    webhook_url: str | None
    max_inline_bytes: int

@dataclass(frozen=True)
class QuartzyConfig:
    enabled: bool
    api_token: str | None
    base_url: str
    org_id: str | None
    lab_id: str | None
    group_id: str | None
    auth_mode: str
    auth_fallback: bool
    orders_page_limit: int
    inventory_scan_pages: int
    enable_partial_status: bool
    inventory_scan_enabled: bool
    cache_ttl_s: int
    inventory_lookup_max_pages: int
    partial_adjust_mode: str
    orders_per_page: int | None = None
    max_workers: int | None = None
    # Optional inventory-specific fast-scan tunables; if unset, fall back to orders_per_page/max_workers
    inventory_per_page: int | None = None
    inventory_max_workers: int | None = None
    # Controls whether API create attempts are used for inventory items (default disabled per current API limits)
    allow_api_inventory_create: bool = False
    # Prefer opening the explicit "/inventory/new" form in the Quartzy app
    prefer_new_form: bool = False

@dataclass(frozen=True)
class AppConfig:
    clickup: ClickUpConfig
    power_automate: PowerAutomateConfig
    quartzy: QuartzyConfig


def _bool(val: str | None, default: bool = False) -> bool:
    if val is None:
        return default
    return val not in ("0", "false", "False")

def load_config() -> AppConfig:
    clickup = ClickUpConfig(
        api_token=os.getenv("CLICKUP_API_TOKEN"),
        team_id=os.getenv("CLICKUP_TEAM_ID"),
        list_id=os.getenv("CLICKUP_LIST_ID"),
        reception_group_id=os.getenv("CLICKUP_RECEPTION_GROUP_ID") or os.getenv("RECEPTION_GROUP_ID"),
        received_status_value=os.getenv("CLICKUP_RECEIVED_STATUS_VALUE"),
        parallel_updates=_bool(os.getenv("CLICKUP_PARALLEL_UPDATES", "1"), True),
        parallel_max_workers=int(os.getenv("CLICKUP_PARALLEL_MAX_WORKERS", "5") or 5),
        cf_project_manager_id=os.getenv("CLICKUP_CF_PROJECT_MANAGER_ID"),
        cf_bsl2_checkbox_id=os.getenv("CLICKUP_CF_BSL2_CHECKBOX_ID", "f394f3db-0812-4ecf-91d8-2ea9a608762e"),
    )
    quartzy = QuartzyConfig(
        enabled=_bool(os.getenv("QUARTZY_ENABLED", "1"), True),
        api_token=os.getenv("QUARTZY_API_TOKEN"),
        base_url=os.getenv("QUARTZY_BASE_URL", "https://api.quartzy.com"),
        org_id=os.getenv("QUARTZY_ORG_ID"),
        lab_id=os.getenv("QUARTZY_LAB_ID", ""),
        group_id=os.getenv("QUARTZY_GROUP_ID"),
        auth_mode=(os.getenv("QUARTZY_AUTH_MODE", "auto").strip().lower()),
        auth_fallback=_bool(os.getenv("QUARTZY_AUTH_FALLBACK", "1"), True),
        orders_page_limit=int(os.getenv("QUARTZY_ORDERS_PAGE_LIMIT", "1") or 1),
        inventory_scan_pages=int(os.getenv("QUARTZY_INVENTORY_SCAN_PAGES", "2") or 2),
        enable_partial_status=_bool(os.getenv("QUARTZY_ENABLE_PARTIAL_STATUS", "1"), True),
        inventory_scan_enabled=_bool(os.getenv("QUARTZY_INVENTORY_SCAN_ENABLED", "1"), True),
        cache_ttl_s=int(os.getenv("QUARTZY_CACHE_TTL_S", "120") or 120),
        inventory_lookup_max_pages=int(os.getenv("QUARTZY_INVENTORY_LOOKUP_MAX_PAGES", "8") or 8),
    # Default to UI automation for partial order adjustments
    partial_adjust_mode=(os.getenv("QUARTZY_PARTIAL_ADJUST_MODE", "ui").strip().lower()),
    orders_per_page=int(os.getenv("QUARTZY_ORDERS_PER_PAGE", "100") or 100),
    max_workers=int(os.getenv("QUARTZY_MAX_WORKERS", "8") or 8),
    inventory_per_page=(
        int(os.getenv("QUARTZY_INVENTORY_PER_PAGE", "0") or 0) or None
    ),
    inventory_max_workers=(
        int(os.getenv("QUARTZY_INVENTORY_MAX_WORKERS", "0") or 0) or None
    ),
    allow_api_inventory_create=_bool(os.getenv("QUARTZY_ALLOW_API_INVENTORY_CREATE", "0"), False),
    prefer_new_form=_bool(os.getenv("QUARTZY_PREFER_NEW_FORM", "0"), False),
    )
    power = PowerAutomateConfig(
        webhook_url=os.getenv("POWER_AUTOMATE_WEBHOOK_URL"),
        max_inline_bytes=int(os.getenv("PA_MAX_INLINE_BYTES", "750000") or 750000),
    )
    return AppConfig(clickup=clickup, power_automate=power, quartzy=quartzy)

CONFIG = load_config()
