# app/services/quartzy_service.py
from __future__ import annotations
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from typing import Optional, Any, Tuple, List, Dict
from app.config import CONFIG as _APP_CONFIG
import difflib
import csv
import os

# Utility
def safe_int(v, default=0):
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip().replace(",", "")
        if s.lstrip("-").isdigit():
            return int(s)
        return default
    except Exception:
        return default


class QuartzyService:
    def __init__(self, config=None):
        cfg = (config or _APP_CONFIG).quartzy
        self.api_token = cfg.api_token
        self.base_url = self._normalize_base(cfg.base_url)
        self.lab_id = cfg.lab_id or ""
        self.enabled = bool(cfg.enabled)
        self.auth_mode = (cfg.auth_mode or "auto").strip().lower()
        self.auth_fallback = bool(cfg.auth_fallback)
        self.orders_page_limit = int(cfg.orders_page_limit or 1)
        self.inventory_scan_pages = int(cfg.inventory_scan_pages or 2)
        self.enable_partial_status = bool(cfg.enable_partial_status)
        self.partial_adjust_mode = (getattr(cfg, "partial_adjust_mode", "api") or "api").strip().lower()
        self.inventory_scan_enabled = bool(cfg.inventory_scan_enabled)
        self.cache_ttl = int(cfg.cache_ttl_s or 120)
        self.inventory_lookup_max_pages = int(cfg.inventory_lookup_max_pages or 8)

        # requests session + retry for connection pooling and improved resilience
        self.session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.4,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "PUT", "POST", "DELETE", "OPTIONS"]),
        )
        # Increase connection pool sizes for higher concurrency under parallel scans
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=64, max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # caches and debug
        self._caches: Dict[str, Any] = {"orders": {"data": None, "ts": 0}, "labs": {"data": None, "ts": 0}, "types": {"data": None, "ts": 0}, "inventory_search": {}}
        self.debug: Dict[str, Any] = {}
        # filter strategy discovery
        self._orders_filter_strategy: Optional[dict] = None
        self._orders_filter_strategy_checked: bool = False

        # defaults for fast fetch
        self.orders_per_page = int(getattr(cfg, "orders_per_page", 100) or 100)
        self.max_workers = int(getattr(cfg, "max_workers", 8) or 8)
        # inventory-specific fast scan tunables (fallback to orders values if not set)
        inv_pp = getattr(cfg, "inventory_per_page", None)
        inv_mw = getattr(cfg, "inventory_max_workers", None)
        try:
            self.inventory_per_page = int(inv_pp) if inv_pp else None
        except Exception:
            self.inventory_per_page = None
        try:
            self.inventory_max_workers = int(inv_mw) if inv_mw else None
        except Exception:
            self.inventory_max_workers = None
        # allow API create attempts? (configurable)
        self.allow_api_inventory_create = bool(getattr(cfg, "allow_api_inventory_create", False))

    def clear_caches(self):
        """Clear all in-memory caches for Quartzy service."""
        try:
            self._caches = {"orders": {"data": None, "ts": 0}, "labs": {"data": None, "ts": 0}, "types": {"data": None, "ts": 0}, "inventory_search": {}}
            # Do not reset debug or discovered strategies; keep them for diagnostics
            return True
        except Exception:
            return False

    # ---------- helpers ----------
    @staticmethod
    def _normalize_base(u: Optional[str]) -> str:
        if not u:
            return "https://api.quartzy.com"
        u = u.strip().rstrip('/')
        if u.lower().endswith('/v2'):
            u = u[:-3]
        return u

    def _headers(self, mode: Optional[str] = None) -> dict:
        mode = mode or ("bearer" if self.auth_mode == "bearer" else "access-token")
        base = {"Accept": "application/json", "Accept-Encoding": "gzip"}
        if self.api_token:
            if mode == "bearer":
                base["Authorization"] = f"Bearer {self.api_token}"
            else:
                base["Access-Token"] = self.api_token
        return base

    def _cache_get(self, key, subkey=None):
        now = time.time()
        if key in ("inventory_search",) and subkey is not None:
            e = self._caches.get(key, {}).get(subkey)
            if not e:
                return None
            if now - e.get("ts", 0) > self.cache_ttl:
                return None
            return e.get("data")
        e = self._caches.get(key)
        if not e:
            return None
        if now - e.get("ts", 0) > self.cache_ttl:
            return None
        return e.get("data")

    def _cache_set(self, key, data, subkey=None):
        now = time.time()
        if key in ("inventory_search",) and subkey is not None:
            self._caches.setdefault(key, {})[subkey] = {"data": data, "ts": now}
        else:
            self._caches[key] = {"data": data, "ts": now}

    # ---------- status token split & param assembly ----------
    def _split_status_tokens(self, statuses: List[str]) -> Tuple[List[str], List[str]]:
        tokens = [str(s).strip() for s in (statuses or []) if str(s).strip()]
        lower = sorted({t.lower() for t in tokens})
        order_set = {"created", "ordered", "received", "cancelled", "canceled", "backordered"}
        approval_set = {"approved", "pending", "rejected"}
        order_tokens = [t for t in lower if t in order_set]
        approval_tokens = [t for t in lower if t in approval_set]
        return order_tokens, approval_tokens

    def _apply_status_params(self, params_base: dict, order_tokens: List[str], approval_tokens: List[str], strategy: Optional[dict]):
        params = dict(params_base or {})
        if not order_tokens and not approval_tokens:
            return params
        if not strategy:
            # multi-shape fallback (send common variants)
            if order_tokens:
                joined = ",".join(order_tokens)
                params.update({"status": joined, "statuses": joined, "status[]": order_tokens + [t.upper() for t in order_tokens]})
            if approval_tokens:
                joined = ",".join(approval_tokens)
                params.update({"approval_status": joined, "approval_statuses": joined, "approval_status[]": approval_tokens + [t.upper() for t in approval_tokens]})
            return params
        case = strategy.get("case", "lower_csv")
        def fmt(vals):
            if case == "upper_csv":
                return ",".join([v.upper() for v in vals])
            if case == "array_lower":
                return vals
            if case == "array_upper":
                return [v.upper() for v in vals]
            return ",".join(vals)
        if order_tokens:
            params[strategy.get("order_key", "status")] = fmt(order_tokens)
        if approval_tokens:
            params[strategy.get("approval_key", "approval_status")] = fmt(approval_tokens)
        return params

    def _ensure_orders_filter_strategy(self, desired_statuses: Optional[List[str]], lab_id: Optional[str]):
        if self._orders_filter_strategy_checked:
            return
        self._orders_filter_strategy_checked = True
        self.debug["orders_filter_discovery_attempted"] = True
        if not desired_statuses:
            self.debug["orders_filter_strategy"] = None
            return
        try:
            order_tokens, approval_tokens = self._split_status_tokens(desired_statuses)
            candidates = [
                {"order_key":"status","approval_key":"approval_status","case":"lower_csv"},
                {"order_key":"statuses","approval_key":"approval_statuses","case":"lower_csv"},
                {"order_key":"status[]","approval_key":"approval_status[]","case":"array_lower"},
                {"order_key":"status","approval_key":"approval_status","case":"upper_csv"},
                {"order_key":"filter[status]","approval_key":"filter[approval_status]","case":"lower_csv"},
            ]
            primary_mode = ("access-token" if self.auth_mode != "bearer" else "bearer")
            path = "/order-requests"
            base: Dict[str, Any] = {"page": 1}
            if lab_id or self.lab_id:
                base["lab_id"] = str(lab_id or self.lab_id)
            unwanted = {"received", "cancelled", "canceled"}
            for strat in candidates:
                params = self._apply_status_params(base, order_tokens, approval_tokens, strat)
                try:
                    resp = self.session.get(f"{self.base_url}{path}", headers=self._headers(primary_mode), params=params, timeout=15)
                    if not resp.ok:
                        continue
                    data = resp.json() or []
                    raw_list = data if isinstance(data, list) else data.get("order_requests") or []
                    obs = {str((o or {}).get("status") or "").strip().lower() for o in raw_list if isinstance(o, dict)}
                    if not obs:
                        self._orders_filter_strategy = strat
                        break
                    if not (obs & unwanted):
                        self._orders_filter_strategy = strat
                        break
                except Exception:
                    continue
            self.debug["orders_filter_strategy"] = self._orders_filter_strategy
        except Exception:
            self.debug["orders_filter_strategy"] = None

    # ---------- core: fetch order requests ----------
    # Common mapping (normalized for UI)
    def _extract_catalog_number(self, o: dict) -> Tuple[Optional[str], Optional[str]]:
        """Try common fields to extract a catalog number consistently.
        Returns (value, source_key)."""
        candidates = [
            "catalog_number",
            "vendor_catalog_number",
            "vendor_product_id",
            "vendor_product",
            "catalog",
            "sku",
            "item_number",
            "product_number",
            "catalogNumber",
        ]
        for k in candidates:
            if k in o and o[k] is not None:
                try:
                    v = str(o[k]).strip()
                    if v:
                        return v, k
                except Exception:
                    continue
        # Deep fields
        details = o.get("details") or {}
        if isinstance(details, dict):
            for k in candidates:
                if k in details and details[k] is not None:
                    try:
                        v = str(details[k]).strip()
                        if v:
                            return v, f"details.{k}"
                    except Exception:
                        continue
        return None, None
    def _map_order(self, o: dict) -> dict:
        item_name = (o.get("item_name") or o.get("name") or "Unnamed").strip()
        vendor_name = o.get("vendor_name") or o.get("vendor") or ""
        qty_str = o.get("quantity")
        qty_expected = safe_int(qty_str)
        cat_no, cat_src = self._extract_catalog_number(o)
        return {
            "id": o.get("id"),
            "name": item_name,
            "vendor": vendor_name,
            "quantity_expected": qty_expected,
            "status": o.get("status") or "",
            "item_name": item_name,
            "vendor_name": vendor_name,
            # normalized catalog number for matching with inventory
            "catalog_number": cat_no,
            "catalog_number_source": cat_src,
            "quantity": qty_str if qty_str is not None else None,
            "unit_size": o.get("unit_size"),
            "unit_price": o.get("unit_price"),
            "total_price": o.get("total_price"),
            "requested_at": o.get("requested_at"),
            "updated_at": o.get("updated_at"),
            "requested_by": o.get("requested_by"),
            "created_by": o.get("created_by"),
            "notes": o.get("notes"),
            "invoice_number": o.get("invoice_number"),
            "requisition_number": o.get("requisition_number"),
            "confirmation_number": o.get("confirmation_number"),
            "tracking_number": o.get("tracking_number"),
            "purchase_order_number": o.get("purchase_order_number"),
            "shipping_and_handling": o.get("shipping_and_handling"),
            "details": o.get("details"),
            "lab": o.get("lab"),
            "type": o.get("type"),
            "spend_tracking_code": o.get("spend_tracking_code"),
            "backordered_expected_at": o.get("backordered_expected_at"),
            "is_urgent": o.get("is_urgent"),
            "app_url": o.get("app_url"),
        }

    def fetch_order_requests(self, statuses: Optional[List[str]] = None, lab_id: Optional[str] = None, all_pages: bool = True, page_limit: Optional[int] = None, safety_max: int = 200):
        """
        Returns: (orders: list[dict], meta: dict)
        """
        desired = [s.strip() for s in (statuses or []) if str(s).strip()]
        if not desired:
            # Default to only ORDERED per latest requirement
            desired = ["ORDERED"]

        if not self.enabled:
            self.debug.update({"last_orders_status": "disabled", "last_orders_count": 0})
            return [], {"pages": 0, "last_status": "disabled"}

        # caching key for short TTL to avoid hammering API on repeated calls
        cache_key = f"orders:{','.join(sorted([d.lower() for d in desired]))}:{lab_id or self.lab_id}:{all_pages}:{page_limit}"
        cached = self._cache_get("orders")  # single-cache for now (optional)
        # note: we keep a simplistic orders cache - you can expand to per-key if you like
        # build param skeleton
        params_base = {}
        lab = lab_id or self.lab_id or None
        if lab:
            params_base["lab_id"] = str(lab)

        # split tokens and decide strategy
        order_tokens, approval_tokens = self._split_status_tokens(desired)
        self.debug["recognized_status_tokens"] = {"order": order_tokens, "approval": approval_tokens}
        try:
            self._ensure_orders_filter_strategy(desired, lab)
        except Exception:
            self.debug["orders_filter_discovery_exception"] = True

        params_with_filters = self._apply_status_params(params_base, order_tokens, approval_tokens, self._orders_filter_strategy)
        self.debug["last_orders_query_params_planned"] = params_with_filters

        # determine auth modes to try
        if self.auth_mode == "bearer":
            modes = ["bearer"]
        elif self.auth_mode in ("access-token", "access_token"):
            modes = ["access-token"]
        else:
            modes = ["access-token", "bearer"] if self.auth_fallback else ["access-token"]

        url = f"{self.base_url}/order-requests"
        attempts = []
        selected_mode = None

        def fetch_page(page: int, mode: str, params_base: dict):
            params = {**params_base, "page": page}
            start = time.perf_counter()
            try:
                resp = self.session.get(url, headers=self._headers(mode), params=params, timeout=30)
                latency = time.perf_counter() - start
                attempts.append({"mode": mode, "http": resp.status_code, "ok": resp.ok, "latency_s": latency, "page": page, "params": params})
                if not resp.ok:
                    return False, [], resp.status_code, params
                data = resp.json() or []
                raw_list = data if isinstance(data, list) else data.get("order_requests") or []
                return True, [self._map_order(o) for o in raw_list if isinstance(o, dict)], resp.status_code, params
            except Exception as ex:
                attempts.append({"mode": mode, "exception": str(ex), "page": page, "params": params})
                return False, [], f"exception:{ex}", params

        orders: List[dict] = []
        pages = 0
        last_status = None
        last_params = None

        # Choose an auth mode that works on the first page
        first_ok = False
        for mode in modes:
            ok, items, status_code, used_params = fetch_page(1, mode, params_with_filters)
            last_status = status_code
            last_params = used_params
            if ok:
                selected_mode = mode
                orders.extend(items)
                pages = 1
                first_ok = True
                break

        if not first_ok:
            self.debug.update({
                "last_orders_status": "http_error" if isinstance(last_status, int) else "exception",
                "last_orders_http": last_status if isinstance(last_status, int) else None,
                "last_orders_error": None if isinstance(last_status, int) else str(last_status),
                "last_orders_count": 0,
                "orders_attempts": attempts,
                "auth_mode_used": None,
                "selected_orders_endpoint": "/order-requests",
            })
            self.debug["last_orders_query_params"] = last_params
            return [], {"pages": pages, "last_status": last_status}

        if not all_pages:
            self.debug.update({
                "last_orders_status": "ok",
                "last_orders_http": 200,
                "last_orders_error": None,
                "last_orders_count": len(orders),
                "orders_attempts": attempts,
                "auth_mode_used": selected_mode,
                "selected_orders_endpoint": "/order-requests",
            })
            self.debug["last_orders_query_params"] = last_params
            self.debug["orders_pages_fetched"] = pages
            self.debug["observed_statuses"] = sorted({(o.get("status") or "").lower() for o in orders})
            return orders, {"pages": pages, "last_status": last_status}

        cap = page_limit if (page_limit and page_limit > 0) else safety_max
        for page in range(2, max(2, cap) + 1):
            use_mode = selected_mode or ("bearer" if self.auth_mode == "bearer" else "access-token")
            ok, items, status_code, used_params = fetch_page(page, use_mode, params_with_filters)
            last_status = status_code
            last_params = used_params
            if not ok or not items:
                break
            orders.extend(items)
            pages += 1

        self.debug.update({
            "last_orders_status": "ok",
            "last_orders_http": 200,
            "last_orders_error": None,
            "last_orders_count": len(orders),
            "orders_attempts": attempts,
            "auth_mode_used": selected_mode,
            "selected_orders_endpoint": "/order-requests",
        })
        self.debug["last_orders_query_params_all"] = {"pages": pages, "last_params": last_params}
        self.debug["orders_pages_fetched"] = pages
        self.debug["observed_statuses"] = sorted({(o.get("status") or "").lower() for o in orders})
        return orders, {"pages": pages, "last_status": last_status}

    # ---------- Inventory: fast fetch + locations ----------
    def _parse_inventory_payload(self, data: Any) -> List[Dict[str, Any]]:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("inventory_items", "inventoryItems", "items", "data", "results"):
                val = data.get(key)
                if isinstance(val, list):
                    return val
        return []

    def _candidate_inventory_endpoints(self, lab_id: Optional[str]) -> List[Tuple[str, Dict[str, Any]]]:
        base = self.base_url.rstrip('/')
        candidates: List[Tuple[str, Dict[str, Any]]] = []
        candidates.append((f"{base}/inventory-items", {"with_lab_id": True}))
        candidates.append((f"{base}/api/v2/inventory-items", {"with_lab_id": True}))
        if lab_id or self.lab_id:
            lid = str(lab_id or self.lab_id)
            candidates.append((f"{base}/labs/{lid}/inventory-items", {"with_lab_id": False}))
            candidates.append((f"{base}/api/v2/labs/{lid}/inventory-items", {"with_lab_id": False}))
        candidates.append((f"{base}/inventory_items", {"with_lab_id": True}))
        if lab_id or self.lab_id:
            lid = str(lab_id or self.lab_id)
            candidates.append((f"{base}/labs/{lid}/inventory_items", {"with_lab_id": False}))
        return candidates

    def _choose_inventory_endpoint(self, lab_id: Optional[str], per_page: int) -> Tuple[Optional[str], bool, List[Dict[str, Any]], Optional[int], Optional[Dict[str, Any]]]:
        attempts: List[Dict[str, Any]] = []
        for ep, meta in self._candidate_inventory_endpoints(lab_id):
            params: Dict[str, Any] = {"page": 1, "per_page": per_page, "limit": per_page}
            if meta.get("with_lab_id") and (lab_id or self.lab_id):
                params["lab_id"] = str(lab_id or self.lab_id)
            try:
                resp = self.session.get(ep, headers=self._headers(), params=params, timeout=15)
                attempts.append({
                    "endpoint": ep,
                    "with_lab_id": bool(meta.get("with_lab_id")),
                    "status": getattr(resp, 'status_code', None),
                    "ok": getattr(resp, 'ok', False),
                    "params": params,
                })
                if resp.status_code == 404 or ("no such operation" in (resp.text or "").lower()):
                    continue
                data = resp.json() if resp.ok else None
            except Exception as ex:
                attempts.append({
                    "endpoint": ep,
                    "with_lab_id": bool(meta.get("with_lab_id")),
                    "exception": str(ex),
                    "params": params,
                })
                continue
            items = self._parse_inventory_payload(data)
            if items is not None:
                # Record the selected endpoint and probe attempts for debugging
                try:
                    self.debug["inventory_endpoint_selected"] = {
                        "endpoint": ep,
                        "with_lab_id": bool(meta.get("with_lab_id")),
                        "status": getattr(resp, 'status_code', None),
                        "ok": getattr(resp, 'ok', False),
                        "headers": dict(getattr(resp, 'headers', {}) or {}),
                        "params": params,
                    }
                    self.debug["inventory_endpoint_probe_attempts"] = attempts
                except Exception:
                    pass
                return ep, bool(meta.get("with_lab_id")), items, resp.status_code, (dict(resp.headers or {}))
        try:
            self.debug["inventory_endpoint_probe_attempts"] = attempts
            self.debug["inventory_endpoint_selected"] = None
        except Exception:
            pass
        return None, False, [], None, None

    def _page_has_next_inventory(self, headers: Dict[str, Any], items_len: int, per_page_hint: int, meta_obj: Optional[Dict[str, Any]] = None) -> Tuple[bool, Optional[int], Optional[int], int]:
        has_next = False
        curr_page = None
        total_pages = None
        x_page = headers.get("X-Page") or headers.get("Page")
        x_total_pages = headers.get("X-Total-Pages") or headers.get("Total-Pages")
        x_next_page = headers.get("X-Next-Page") or headers.get("Next-Page")
        eff_page_size = None
        for k in ("X-Per-Page", "Per-Page", "x-per-page", "x_per_page"):
            if k in headers:
                try:
                    eff_page_size = int(headers[k]); break
                except Exception:
                    pass
        if meta_obj and not eff_page_size:
            for k in ("per_page", "page_size", "perPage", "pageSize"):
                if k in meta_obj:
                    try:
                        eff_page_size = int(meta_obj[k]); break
                    except Exception:
                        pass
        # IMPORTANT: If the API doesn't advertise page size, default to 25
        # rather than trusting our requested per_page (service may cap silently).
        if eff_page_size is None:
            eff_page_size = 25
        try:
            if x_page: curr_page = int(x_page)
            if x_total_pages: total_pages = int(x_total_pages)
        except Exception:
            pass
        if not has_next and x_total_pages and x_page:
            try:
                has_next = int(x_page) < int(x_total_pages)
            except Exception:
                pass
        if not has_next and x_next_page:
            has_next = True
        if not has_next:
            link_hdr = headers.get("Link") or headers.get("link")
            if link_hdr and 'rel="next"' in str(link_hdr).lower():
                has_next = True
        if not has_next and total_pages is not None and curr_page is not None:
            has_next = curr_page < total_pages
        if not has_next:
            has_next = items_len == eff_page_size
        return has_next, curr_page, total_pages, eff_page_size

    def _list_inventory_page(self, base_endpoint: str, lab_id: Optional[str], with_lab_id_param: bool, page: int, per_page: int) -> Tuple[List[Dict[str, Any]], bool, Dict[str, Any]]:
        params: Dict[str, Any] = {"page": page, "per_page": per_page, "limit": per_page}
        if with_lab_id_param and (lab_id or self.lab_id):
            params["lab_id"] = str(lab_id or self.lab_id)
        resp = self.session.get(base_endpoint, headers=self._headers(), params=params, timeout=15)
        if resp is None or not getattr(resp, 'ok', False):
            return [], False, {"page": page, "total_pages": None, "eff_page_size": None}
        try:
            data = resp.json()
        except Exception:
            data = {}
        items = self._parse_inventory_payload(data)
        meta = {}
        if isinstance(data, dict):
            meta = data.get("meta") or data.get("pagination") or {}
        has_next, curr_page, total_pages, eff_page_size = self._page_has_next_inventory(dict(resp.headers or {}), len(items), per_page, meta)
        return items, has_next, {"page": curr_page or page, "total_pages": total_pages, "eff_page_size": eff_page_size}

    def fetch_inventory_items_fast(self, lab_id: Optional[str], per_page: Optional[int] = None, max_workers: Optional[int] = None) -> List[Dict[str, Any]]:
        lab = lab_id or self.lab_id or None
        # prefer provided per_page; else inventory-specific; else orders default
        eff_per_page = int(per_page or (self.inventory_per_page or self.orders_per_page))
        eff_workers = int(max_workers or (self.inventory_max_workers or self.max_workers))
        try:
            self.debug["inventory_fetch_config"] = {"lab_id": str(lab or ""), "per_page": eff_per_page, "max_workers": eff_workers}
        except Exception:
            pass
        ep, with_lab_id, first_items, _, headers = self._choose_inventory_endpoint(lab, eff_per_page)
        if not ep:
            return []
        _, has_next, meta = self._list_inventory_page(ep, lab, with_lab_id, page=1, per_page=eff_per_page)
        results: List[Dict[str, Any]] = list(first_items)
        total_pages = meta.get("total_pages")
        if not has_next and (not total_pages or total_pages == 1):
            return results
        if not total_pages:
            page = 2
            while True:
                items, has_next, _ = self._list_inventory_page(ep, lab, with_lab_id, page=page, per_page=eff_per_page)
                results.extend(items)
                if not has_next:
                    break
                page += 1
            return results
        from concurrent.futures import ThreadPoolExecutor, as_completed
        pages = list(range(2, int(total_pages) + 1))
        with ThreadPoolExecutor(max_workers=eff_workers) as ex:
            futs = {ex.submit(self._list_inventory_page, ep, lab, with_lab_id, p, eff_per_page): p for p in pages}
            for fut in as_completed(futs):
                try:
                    items, _, _ = fut.result()
                except Exception:
                    continue
                results.extend(items)
        return results

    def _as_name(self, val: Any) -> str:
        if val is None:
            return ""
        if isinstance(val, str):
            return val.strip()
        if isinstance(val, dict):
            for k in ("name", "label", "title", "display_name", "displayName"):
                if k in val and val[k] is not None:
                    try:
                        return str(val[k]).strip()
                    except Exception:
                        continue
            for k in ("location", "sub_location", "sublocation"):
                if k in val and val[k] is not None:
                    nested = self._as_name(val[k])
                    if nested:
                        return nested
        try:
            return str(val).strip()
        except Exception:
            return ""

    def _extract_location_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        LOCATION_KEYS = ["location", "location_name", "locationName", "storage_location", "storageLocation", "storage"]
        SUBLOCATION_KEYS = ["sublocation", "sub_location", "sublocation_name", "sub_location_name", "subLocation", "box", "shelf", "drawer", "rack", "bin"]
        loc = ""
        subloc = ""
        for k in LOCATION_KEYS:
            if k in item and item[k] is not None:
                loc = self._as_name(item[k])
                if loc:
                    break
        for k in SUBLOCATION_KEYS:
            if k in item and item[k] is not None:
                subloc = self._as_name(item[k])
                if subloc:
                    break
        if not loc:
            for k in ("storage", "location_info", "locationInfo"):
                if k in item and isinstance(item[k], dict):
                    loc = self._as_name(item[k].get("location") or item[k].get("name"))
                    if not subloc:
                        subloc = self._as_name(item[k].get("sublocation") or item[k].get("sub_location"))
                    if loc or subloc:
                        break
        return {
            "inventory_item_id": item.get("id"),
            "item_name": item.get("name") or item.get("item_name") or item.get("display_name"),
            "location": loc,
            "sub_location": subloc,
        }

    def collect_lab_locations_fast(self, lab_id: Optional[str] = None, per_page: Optional[int] = None, refresh: bool = False, max_workers: Optional[int] = None):
        if not self.enabled or not self.api_token:
            return {"locations": [], "sub_locations": [], "sample_size": 0, "disabled": True}
        # lightweight TTL cache
        key = f"lab_locations_fast:{lab_id or self.lab_id}:{per_page or (self.inventory_per_page or self.orders_per_page)}:{max_workers or (self.inventory_max_workers or self.max_workers)}"
        if not refresh:
            cached = self._cache_get(key)
            if cached is not None:
                return cached

        lab = lab_id or self.lab_id or None
        per_page_val = int(per_page or (self.inventory_per_page or self.orders_per_page))
        eff_workers = int(max_workers or (self.inventory_max_workers or self.max_workers))

        # Discover a working inventory endpoint and prime page 1
        ep, with_lab_id, first_items, _, _ = self._choose_inventory_endpoint(lab, per_page_val)
        if not ep:
            return {"locations": [], "sub_locations": [], "sample_size": 0, "lab_id": str(lab or "")}

        # Page 1 metadata (for total_pages discovery)
        _, has_next, meta = self._list_inventory_page(ep, lab, with_lab_id, page=1, per_page=per_page_val)

        # Initialize sets and mapping with page 1
        loc_set: set = set()
        sub_set: set = set()
        loc_to_sub: Dict[str, set] = {}
        scanned = 0
        for it in first_items:
            scanned += 1
            rec = self._extract_location_record(it)
            loc = (rec.get("location") or "").strip()
            sub = (rec.get("sub_location") or "").strip()
            if loc:
                loc_set.add(loc)
                if sub:
                    loc_to_sub.setdefault(loc, set()).add(sub)
            if sub:
                sub_set.add(sub)

        total_pages = meta.get("total_pages")
        if not has_next and (not total_pages or total_pages == 1):
            result = {
                "locations": sorted(loc_set),
                "sub_locations": sorted(sub_set),
                "location_to_sublocations": {k: sorted(v) for k, v in loc_to_sub.items()},
                "sample_size": scanned,
                "lab_id": str(lab or ""),
            }
            self._cache_set(key, result)
            return result

        # Helper to process a page and update accumulators
        def _process_page(p: int) -> Tuple[int, int]:
            items, _, _ = self._list_inventory_page(ep, lab, with_lab_id, page=p, per_page=per_page_val)
            added = 0
            for it in items:
                nonlocal_scanned = 1  # placeholder for clarity
                rec = self._extract_location_record(it)
                loc = (rec.get("location") or "").strip()
                sub = (rec.get("sub_location") or "").strip()
                if loc:
                    loc_set.add(loc)
                    if sub:
                        loc_to_sub.setdefault(loc, set()).add(sub)
                if sub:
                    sub_set.add(sub)
                added += 1
            return p, added

        # If total pages known, parallelize pages 2..N
        if total_pages and int(total_pages) > 1:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            pages = list(range(2, int(total_pages) + 1))
            with ThreadPoolExecutor(max_workers=eff_workers) as ex:
                futs = {ex.submit(self._list_inventory_page, ep, lab, with_lab_id, p, per_page_val): p for p in pages}
                for fut in as_completed(futs):
                    p = futs[fut]
                    try:
                        items, _, _ = fut.result()
                    except Exception:
                        items = []
                    scanned += len(items)
                    for it in items:
                        rec = self._extract_location_record(it)
                        loc = (rec.get("location") or "").strip()
                        sub = (rec.get("sub_location") or "").strip()
                        if loc:
                            loc_set.add(loc)
                            if sub:
                                loc_to_sub.setdefault(loc, set()).add(sub)
                        if sub:
                            sub_set.add(sub)
        else:
            # Unknown total pages: stride-based windowed parallel scan
            from concurrent.futures import ThreadPoolExecutor, as_completed
            stride = max(1, int(eff_workers))
            start_page = 2
            end_page = 1 + stride
            initial_pages = list(range(start_page, end_page + 1))
            # Early-stop heuristic: if unique locations and sub-locations don't grow
            # for a number of consecutive page fetches, stop scheduling more pages.
            no_growth_streak = 0
            last_unique_total = len(loc_set) + len(sub_set)

            def fetch_rows_for_page(p: int) -> Tuple[int, int, int]:
                items, _, _ = self._list_inventory_page(ep, lab, with_lab_id, page=p, per_page=per_page_val)
                return p, len(items), 0 if items is None else 1  # flag 1 if fetched

            with ThreadPoolExecutor(max_workers=stride) as ex:
                futs = {ex.submit(self._list_inventory_page, ep, lab, with_lab_id, p, per_page=per_page_val): p for p in initial_pages}
                while futs:
                    for fut in as_completed(list(futs.keys())):
                        p = futs.pop(fut)
                        try:
                            items, _, _ = fut.result()
                        except Exception:
                            items = []
                        scanned += len(items)
                        # Update accumulators
                        grew = False
                        for it in items:
                            rec = self._extract_location_record(it)
                            loc = (rec.get("location") or "").strip()
                            sub = (rec.get("sub_location") or "").strip()
                            if loc:
                                before = (loc not in loc_set)
                                loc_set.add(loc)
                                if sub:
                                    before_sub = (sub not in (loc_to_sub.setdefault(loc, set())))
                                    loc_to_sub.setdefault(loc, set()).add(sub)
                                    grew = grew or before or before_sub
                                else:
                                    grew = grew or before
                            if sub:
                                before_s = (sub not in sub_set)
                                sub_set.add(sub)
                                grew = grew or before_s
                        # Early-stop detection
                        current_unique_total = len(loc_set) + len(sub_set)
                        if current_unique_total <= last_unique_total and len(items) > 0:
                            no_growth_streak += 1
                        else:
                            no_growth_streak = 0
                            last_unique_total = current_unique_total
                        # Schedule next page unless we've hit a no-growth streak
                        if len(items) > 0 and no_growth_streak < 3:
                            next_p = p + stride
                            futs[ex.submit(self._list_inventory_page, ep, lab, with_lab_id, next_p, per_page=per_page_val)] = next_p

        result = {
            "locations": sorted(loc_set),
            "sub_locations": sorted(sub_set),
            "location_to_sublocations": {k: sorted(v) for k, v in loc_to_sub.items()},
            "sample_size": scanned,
            "lab_id": str(lab or ""),
        }
        self._cache_set(key, result)
        return result

    # ---------- Inventory match + create ----------
    def find_inventory_match(self, item_name: Optional[str] = None, vendor_name: Optional[str] = None, catalog_number: Optional[str] = None, lab_id: Optional[str] = None) -> Dict[str, Any]:
        items = self.fetch_inventory_items_fast(lab_id)
        name_norm = (item_name or "").strip().lower()
        vendor_norm = (vendor_name or "").strip().lower()
        cat_norm = (catalog_number or "").strip().lower()
        best = None
        for it in items:
            nm = ((it.get("name") or it.get("item_name") or "").strip().lower())
            vn = ((it.get("vendor") or it.get("vendor_name") or "").strip().lower())
            cn = ((it.get("catalog_number") or it.get("vendor_product_id") or it.get("vendor_product") or "").strip().lower())
            if cat_norm and cn and cat_norm == cn:
                best = it
                break
            if name_norm and vendor_norm and nm == name_norm and vn == vendor_norm:
                best = it
                break
            if name_norm and nm == name_norm:
                best = it
        if best:
            rec = self._extract_location_record(best)
            return {"found": True, "item": best, "location": rec.get("location"), "sub_location": rec.get("sub_location")}
        return {"found": False}

    def create_inventory_item(self, item_name: str, vendor_name: Optional[str] = None, catalog_number: Optional[str] = None, quantity: Optional[str] = None, location: Optional[str] = None, sub_location: Optional[str] = None, lab_id: Optional[str] = None) -> Dict[str, Any]:
        """Attempt to create a new inventory item using multiple endpoint/body variants.
        Returns a diagnostic payload with attempts log and, when successful, the created item response.
        """
        nm = (item_name or "").strip()
        if not nm:
            return {"created": False, "error": "item_name required"}
        base = self.base_url.rstrip('/')
        lid = (str(lab_id or self.lab_id).strip() or None)
        # Normalize quantity to a string when provided
        qty_str: Optional[str] = None
        if quantity is not None:
            try:
                qty_str = str(int(str(quantity).strip().split(".")[0]))
            except Exception:
                qty_str = str(quantity)
        paths = [
            f"{base}/inventory-items",
            f"{base}/api/v2/inventory-items",
            f"{base}/inventory_items",
            f"{base}/api/v2/inventory_items",
        ]
        if lid:
            paths.extend([
                f"{base}/labs/{lid}/inventory-items",
                f"{base}/api/v2/labs/{lid}/inventory-items",
                f"{base}/labs/{lid}/inventory_items",
                f"{base}/api/v2/labs/{lid}/inventory_items",
            ])
        headers = {**self._headers(), "Content-Type": "application/json", "Accept": "application/json"}
        # Base field sets (progressively reduced to avoid schema rejections)
        base_fields = {"name": nm}
        if vendor_name: base_fields["vendor"] = vendor_name
        if catalog_number: base_fields["catalog_number"] = catalog_number
        if lid: base_fields["lab_id"] = lid
        if qty_str is not None: base_fields["quantity"] = qty_str
        # Location variants (try later): omit first, then include as strings/objects
        location_variants: List[Dict[str, Any]] = []
        # no location first
        location_variants.append({})
        if location:
            location_variants.append({"location": {"name": location}})
            location_variants.append({"location": location})
        if sub_location:
            location_variants.append({"sublocation": {"name": sub_location}})
            location_variants.append({"sub_location": {"name": sub_location}})
            location_variants.append({"sublocation": sub_location})
            location_variants.append({"sub_location": sub_location})
        # Body wrappers and vendor alt key
        bodies: List[Dict[str, Any]] = []
        # 1) Direct body
        bodies.append(dict(base_fields))
        # 1b) vendor_name alt
        if "vendor" in base_fields:
            alt = dict(base_fields)
            alt["vendor_name"] = alt.pop("vendor")
            bodies.append(alt)
        # 2) Wrapped body
        bodies.append({"inventory_item": dict(base_fields)})
        # 2b) Wrapped with vendor_name alt
        if "vendor" in base_fields:
            walt = {"inventory_item": dict(base_fields)}
            walt["inventory_item"]["vendor_name"] = walt["inventory_item"].pop("vendor")
            bodies.append(walt)
        # Try JSON:API last
        jsonapi_bodies: List[Dict[str, Any]] = []
        attrs = {k: v for k, v in base_fields.items() if k not in ("lab_id",)}
        jsonapi = {"data": {"type": "inventory-items", "attributes": attrs}}
        if lid:
            jsonapi["data"]["relationships"] = {"lab": {"data": {"type": "labs", "id": lid}}}
        jsonapi_bodies.append(jsonapi)
        tries: List[Dict[str, Any]] = []
        # Try non-JSON:API first, layering in location variants progressively
        for path in paths:
            for core in bodies:
                for locv in location_variants:
                    body = {}
                    # merge core + loc variant appropriately (respect wrapper)
                    if "inventory_item" in core:
                        inner = dict(core["inventory_item"]) ; inner.update({k: v for k, v in locv.items()})
                        body = {"inventory_item": {k: v for k, v in inner.items() if v is not None}}
                    else:
                        body = {**core, **locv}
                        body = {k: v for k, v in body.items() if v is not None}
                    try:
                        resp = self.session.post(path, headers=headers, json=body, timeout=25)
                        entry = {"url": path, "method": "POST", "status": getattr(resp, 'status_code', None), "ok": getattr(resp, 'ok', None), "body_shape": list(body.keys())}
                        try:
                            if hasattr(resp, 'ok') and not resp.ok:
                                entry["resp_snippet"] = (resp.text or "")[:240]
                        except Exception:
                            pass
                        tries.append(entry)
                        if getattr(resp, 'ok', False):
                            try:
                                data = resp.json() or {}
                            except Exception:
                                data = {"text": resp.text[:400] if hasattr(resp, 'text') else None}
                            return {"created": True, "http": resp.status_code, "response": data, "request": body, "path": path, "attempts": tries}
                        # Some paths may 404; try next path quickly
                        if getattr(resp, 'status_code', None) == 404:
                            break
                    except Exception as e:
                        tries.append({"url": path, "method": "POST", "exception": str(e), "body_shape": list((body or {}).keys())})
        # Try JSON:API style
        jsonapi_headers = {**self._headers(), "Content-Type": "application/vnd.api+json", "Accept": "application/vnd.api+json"}
        for path in paths:
            for jb in jsonapi_bodies:
                try:
                    resp = self.session.post(path, headers=jsonapi_headers, json=jb, timeout=25)
                    entry = {"url": path, "method": "POST", "status": getattr(resp, 'status_code', None), "ok": getattr(resp, 'ok', None), "body_shape": ["data"], "content_type": "application/vnd.api+json"}
                    try:
                        if hasattr(resp, 'ok') and not resp.ok:
                            entry["resp_snippet"] = (resp.text or "")[:240]
                    except Exception:
                        pass
                    tries.append(entry)
                    if getattr(resp, 'ok', False):
                        try:
                            data = resp.json() or {}
                        except Exception:
                            data = {"text": resp.text[:400] if hasattr(resp, 'text') else None}
                        return {"created": True, "http": resp.status_code, "response": data, "request": jb, "path": path, "attempts": tries}
                except Exception as e:
                    tries.append({"url": path, "method": "POST", "exception": str(e), "body_shape": ["data"], "content_type": "application/vnd.api+json"})
        # Build a prefill link for UI fallback
        link = self.build_quartzy_add_link(item_name=nm, vendor_name=vendor_name, catalog_number=catalog_number, location=location, lab_id=lab_id)
        return {"created": False, "attempts": tries, "error": "no_variant_succeeded", "prefill_url": link}

    def try_update_item_location(self, item_id: str, location: Optional[str] = None, sub_location: Optional[str] = None) -> Dict[str, Any]:
        if not item_id or (not location and not sub_location):
            return {"updated": False, "skipped": True}
        id_str = str(item_id).strip()
        base = self.base_url.rstrip('/')
        paths = [
            f"{base}/inventory-items/{id_str}",
            f"{base}/api/v2/inventory-items/{id_str}",
            f"{base}/inventory_items/{id_str}",
        ]
        headers = {**self._headers(), "Content-Type": "application/json", "Accept": "application/json"}
        variants: List[Dict[str, Any]] = []
        core: Dict[str, Any] = {}
        if location: core["location"] = {"name": location}
        if sub_location:
            core["sublocation"] = {"name": sub_location}
            variants.append(core)
            variants.append({"inventory_item": dict(core)})
        else:
            variants.append(core)
            variants.append({"inventory_item": dict(core)})
        tries: List[Dict[str, Any]] = []
        for p in paths:
            for body in variants:
                try:
                    resp = self.session.patch(p, headers=headers, json=body, timeout=20)
                    entry = {"url": p, "method": "PATCH", "status": getattr(resp, 'status_code', None), "ok": getattr(resp, 'ok', None), "body_shape": list(body.keys())}
                    tries.append(entry)
                    if getattr(resp, 'ok', False):
                        return {"updated": True, "http": resp.status_code, "attempts": tries}
                except Exception as e:
                    tries.append({"url": p, "method": "PATCH", "exception": str(e)})
        return {"updated": False, "attempts": tries}

    def create_or_update_inventory(self, item_name: str, vendor_name: Optional[str] = None, catalog_number: Optional[str] = None, quantity: Optional[str] = None, location: Optional[str] = None, sub_location: Optional[str] = None, lab_id: Optional[str] = None, if_exists: str = "update", allow_api_create: Optional[bool] = None) -> Dict[str, Any]:
        """High-level helper: find-or-create inventory item.
        - If a matching item exists and if_exists=="update" and quantity provided, updates quantity.
        - Otherwise, attempt create; on success, optionally set location via PATCH.
        - Returns a structured report with created/updated flags and diagnostic attempts.
        """
        # 1) Duplicate check
        try:
            match = self.find_inventory_match(item_name=item_name, vendor_name=vendor_name, catalog_number=catalog_number, lab_id=lab_id)
        except Exception:
            match = {"found": False}
        if isinstance(match, dict) and bool(match.get("found")):
            item_obj = match.get("item") if isinstance(match.get("item"), dict) else {}
            item_id = None
            if isinstance(item_obj, dict):
                item_id = item_obj.get("id") or item_obj.get("inventory_item_id") or item_obj.get("raw_id")
            if if_exists == "update" and quantity is not None and item_id:
                upd = self.update_inventory_item(item_id=str(item_id), quantity=str(quantity))
                res = {"exists": True, "updated": bool(upd.get("updated")), "match": match, "update_result": upd}
                return res
            return {"exists": True, "skipped": True, "match": match}
        # 2) Create (if allowed by config)
        allow_create = self.allow_api_inventory_create if allow_api_create is None else bool(allow_api_create)
        if not allow_create:
            link = self.build_quartzy_add_link(item_name=item_name, vendor_name=vendor_name, catalog_number=catalog_number, location=location, lab_id=lab_id)
            return {"created": False, "exists": False, "needs_ui_add": True, "prefill_url": link}
        created = self.create_inventory_item(item_name=item_name, vendor_name=vendor_name, catalog_number=catalog_number, quantity=str(quantity) if quantity is not None else None, location=location, sub_location=sub_location, lab_id=lab_id)
        if created.get("created"):
            # Try to extract id for optional location update/verification
            new_id = None
            resp = created.get("response") or {}
            if isinstance(resp, dict):
                for k in ("id", "inventory_item", "item", "data"):
                    v = resp.get(k)
                    if isinstance(v, dict) and v.get("id"):
                        new_id = v.get("id")
                        break
                    if k == "id" and v:
                        new_id = v
                        break
            # If not already included in create and we have an id, attempt location update
            if new_id and (location or sub_location):
                try:
                    self.try_update_item_location(str(new_id), location=location, sub_location=sub_location)
                except Exception:
                    pass
            return {"created": True, "result": created}
        # 3) Failure: provide prefill URL for UI fallback
        return {"created": False, "result": created, "prefill_url": created.get("prefill_url")}

    def update_inventory_item(self, item_id: str, item_name: Optional[str] = None, vendor_name: Optional[str] = None, catalog_number: Optional[str] = None, quantity: Optional[str] = None, location: Optional[str] = None, sub_location: Optional[str] = None, quantity_delta: Optional[int] = None) -> Dict[str, Any]:
        if not item_id:
            return {"updated": False, "error": "missing_item_id"}
        # Normalize quantity to int when possible
        qty_int: Optional[int] = None
        if quantity is not None:
            try:
                qty_int = int(str(quantity).strip().split(".")[0])
            except Exception:
                qty_int = None
        id_str = str(item_id).strip()
        base = self.base_url.rstrip('/')
        
        # Per Quartzy API spec: PUT /inventory-items/{id} with JSON body {"quantity": "string"}
        # Try the canonical request first with quantity as a string, Content-Type: application/json
        headers = {**self._headers(), "Content-Type": "application/json", "Accept": "application/json"}
        tries: List[Dict[str, Any]] = []
        if qty_int is not None:
            primary_url = f"{base}/inventory-items/{id_str}"
            primary_body = {"quantity": str(int(qty_int))}
            try:
                presp = self.session.put(primary_url, headers=headers, json=primary_body, timeout=25)
                entry = {"url": primary_url, "method": "PUT", "status": getattr(presp, 'status_code', None), "ok": getattr(presp, 'ok', None), "body_shape": list(primary_body.keys())}
                try:
                    if hasattr(presp, 'ok') and not presp.ok:
                        snippet = None
                        try:
                            snippet = presp.text[:200]
                        except Exception:
                            snippet = None
                        if snippet:
                            entry["resp_snippet"] = snippet
                except Exception:
                    pass
                tries.append(entry)
                if getattr(presp, 'ok', False):
                    try:
                        pdata = presp.json()
                    except Exception:
                        pdata = {"text": presp.text[:400] if hasattr(presp, 'text') else None}
                    return {"updated": True, "http": presp.status_code, "response": pdata, "request": primary_body, "path": primary_url, "method": "PUT", "attempts": tries}
            except Exception as ep:
                tries.append({"url": primary_url, "method": "PUT", "exception": str(ep), "body_shape": ["quantity"]})
        paths = [
            f"{base}/inventory-items/{id_str}",
            f"{base}/api/v2/inventory-items/{id_str}",
            f"{base}/inventory_items/{id_str}",
            f"{base}/api/v2/inventory_items/{id_str}",
        ]
        # Some tenants may require lab-scoped item update paths
        lid = (self.lab_id or "").strip()
        if lid:
            paths.extend([
                f"{base}/labs/{lid}/inventory-items/{id_str}",
                f"{base}/api/v2/labs/{lid}/inventory-items/{id_str}",
                f"{base}/labs/{lid}/inventory_items/{id_str}",
                f"{base}/api/v2/labs/{lid}/inventory_items/{id_str}",
            ])
    # Continue with broader fallbacks only if the canonical call didn't succeed
        # Build field bundles, prioritizing minimal bodies to avoid schema rejections
        flat_fields: Dict[str, Any] = {}
        if item_name: flat_fields["name"] = item_name
        if vendor_name: flat_fields["vendor"] = vendor_name
        if catalog_number: flat_fields["catalog_number"] = catalog_number
        # Location fields often require IDs; avoid sending unless necessary
        # if location: flat_fields["location"] = {"name": location}
        # if sub_location: flat_fields["sublocation"] = {"name": sub_location}

        body_variants: List[Dict[str, Any]] = []
        # 1) quantity-only bodies (most permissive)
        if qty_int is not None:
            # Prefer correct schema first: quantity as string per spec
            b0 = {"quantity": str(int(qty_int))}
            b1 = {"quantity": int(qty_int)}  # legacy/int variant
            b2 = {"inventory_item": {"quantity": int(qty_int)}}
            b3 = {"quantity_in_stock": int(qty_int)}
            b4 = {"inventory_item": {"quantity_in_stock": int(qty_int)}}
            body_variants.extend([b0, b1, b2, b3, b4])
        # 2) If additional fields provided, try alongside quantity
        if flat_fields:
            with_qty = dict(flat_fields)
            if qty_int is not None:
                with_qty["quantity"] = qty_int
            body_variants.append(with_qty)
            body_variants.append({"inventory_item": with_qty})
        # 3) As a last resort, try just the non-quantity fields
        if flat_fields:
            ff = dict(flat_fields)
            body_variants.append(ff)
            body_variants.append({"inventory_item": ff})
        for url in paths:
            for body in body_variants:
                # Try PUT then PATCH
                for method in ("PUT", "PATCH"):
                    try:
                        if method == "PUT":
                            resp = self.session.put(url, headers=headers, json=body, timeout=25)
                        else:
                            resp = self.session.patch(url, headers=headers, json=body, timeout=25)
                        entry = {"url": url, "method": method, "status": getattr(resp, 'status_code', None), "ok": getattr(resp, 'ok', None), "body_shape": list(body.keys())}
                        # Include short response snippet for debugging on errors
                        try:
                            if hasattr(resp, 'ok') and not resp.ok:
                                snippet = None
                                try:
                                    snippet = resp.text[:200]
                                except Exception:
                                    snippet = None
                                if snippet:
                                    entry["resp_snippet"] = snippet
                        except Exception:
                            pass
                        tries.append(entry)
                        if getattr(resp, 'ok', False):
                            try:
                                data = resp.json()
                            except Exception:
                                data = {"text": resp.text[:400] if hasattr(resp, 'text') else None}
                            return {"updated": True, "http": resp.status_code, "response": data, "request": body, "path": url, "method": method, "attempts": tries}
                        # If 404 on this path, move to next path variant immediately
                        if getattr(resp, 'status_code', None) == 404:
                            break
                    except Exception as e:
                        tries.append({"url": url, "method": method, "exception": str(e), "body_shape": list(body.keys())})

        # Fallback A: Try PUT with the full item payload fetched via GET, with only quantity changed
        try:
            # Only attempt if we have a concrete quantity to set
            if qty_int is not None:
                # First, fetch the existing item payload from a working GET path
                get_paths = [
                    f"{base}/inventory-items/{id_str}",
                    f"{base}/api/v2/inventory-items/{id_str}",
                    f"{base}/inventory_items/{id_str}",
                ]
                fetched_item: Optional[Dict[str, Any]] = None
                get_path_used: Optional[str] = None
                for gp in get_paths:
                    try:
                        gresp = self.session.get(gp, headers=self._headers(), timeout=20)
                        tries.append({"url": gp, "method": "GET", "status": getattr(gresp, 'status_code', None), "ok": getattr(gresp, 'ok', None)})
                        if getattr(gresp, 'ok', False):
                            data = {}
                            try:
                                data = gresp.json() or {}
                            except Exception:
                                data = {}
                            # Some APIs wrap the object under a key (e.g., inventory_item)
                            core = data
                            if isinstance(data, dict):
                                for k in ("inventory_item", "item", "inventoryItem"):
                                    if isinstance(data.get(k), dict):
                                        core = data.get(k)
                                        break
                            if isinstance(core, dict):
                                fetched_item = dict(core)
                                get_path_used = gp
                                break
                    except Exception as eg:
                        tries.append({"url": gp, "method": "GET", "exception": str(eg)})
                if fetched_item is not None:
                    # Update the most likely quantity fields in-place
                    candidate_qty_keys = [
                        "quantity",
                        "quantity_in_stock",
                        "qty",
                        "in_stock_quantity",
                    ]
                    updated_any_field = False
                    for k in candidate_qty_keys:
                        if k in fetched_item:
                            try:
                                fetched_item[k] = int(qty_int)
                                updated_any_field = True
                            except Exception:
                                pass
                    if not updated_any_field:
                        # If no known key existed, set a conservative "quantity"
                        fetched_item["quantity"] = int(qty_int)
                    # Avoid sending clearly read-only identifiers if present
                    for ro_key in ("id", "app_url", "created_at", "updated_at", "url"):
                        if ro_key in fetched_item:
                            try:
                                # Keep ids in case API requires them; only strip URLs/timestamps
                                if ro_key in ("app_url", "url", "created_at", "updated_at"):
                                    fetched_item.pop(ro_key, None)
                            except Exception:
                                pass
                    # Try PUT with raw object and with wrapper
                    put_bodies = [fetched_item, {"inventory_item": fetched_item}]
                    # Prefer the same path used for GET if available, else the canonical hyphen path
                    put_paths = [get_path_used] if get_path_used else []
                    if not put_paths or put_paths[0] is None:
                        put_paths = [f"{base}/inventory-items/{id_str}"]
                    for purl in put_paths:
                        for body in put_bodies:
                            try:
                                presp = self.session.put(purl, headers=headers, json=body, timeout=25)
                                entry = {"url": purl, "method": "PUT", "status": getattr(presp, 'status_code', None), "ok": getattr(presp, 'ok', None), "body_shape": list(body.keys())}
                                try:
                                    if hasattr(presp, 'ok') and not presp.ok:
                                        snippet = None
                                        try:
                                            snippet = presp.text[:200]
                                        except Exception:
                                            snippet = None
                                        if snippet:
                                            entry["resp_snippet"] = snippet
                                except Exception:
                                    pass
                                tries.append(entry)
                                if getattr(presp, 'ok', False):
                                    try:
                                        pdata = presp.json()
                                    except Exception:
                                        pdata = {"text": presp.text[:400] if hasattr(presp, 'text') else None}
                                    return {"updated": True, "http": presp.status_code, "response": pdata, "request": body, "path": purl, "method": "PUT", "attempts": tries}
                            except Exception as ep:
                                tries.append({"url": purl, "method": "PUT", "exception": str(ep), "body_shape": list(body.keys())})
        except Exception:
            # swallow and continue to next fallback
            pass

        # Fallback B: Try JSON:API format with application/vnd.api+json
        # Some tenants may require JSON:API bodies like {"data": {"type": "inventory-items", "id": id, "attributes": {"quantity": N}}}
        try:
            if qty_int is not None:
                jsonapi_headers = {**self._headers(), "Content-Type": "application/vnd.api+json", "Accept": "application/vnd.api+json"}
                jsonapi_paths = [f"{base}/inventory-items/{id_str}"]
                if lid:
                    jsonapi_paths.append(f"{base}/labs/{lid}/inventory-items/{id_str}")
                type_variants = ["inventory-items", "inventory_items"]
                qty_keys = ["quantity", "quantity_in_stock"]
                for purl in jsonapi_paths:
                    for tval in type_variants:
                        for qk in qty_keys:
                            body = {
                                "data": {
                                    "type": tval,
                                    "id": id_str,
                                    "attributes": {qk: int(qty_int)},
                                }
                            }
                            # Include lab relationship if lab id is available
                            if lid:
                                body["data"]["relationships"] = {
                                    "lab": {"data": {"type": "labs", "id": lid}}
                                }
                            for method in ("PATCH", "PUT"):
                                try:
                                    if method == "PATCH":
                                        resp = self.session.patch(purl, headers=jsonapi_headers, json=body, timeout=25)
                                    else:
                                        resp = self.session.put(purl, headers=jsonapi_headers, json=body, timeout=25)
                                    entry = {"url": purl, "method": method, "status": getattr(resp, 'status_code', None), "ok": getattr(resp, 'ok', None), "body_shape": ["data"], "content_type": "application/vnd.api+json"}
                                    try:
                                        if hasattr(resp, 'ok') and not resp.ok:
                                            snippet = None
                                            try:
                                                snippet = resp.text[:200]
                                            except Exception:
                                                snippet = None
                                            if snippet:
                                                entry["resp_snippet"] = snippet
                                    except Exception:
                                        pass
                                    tries.append(entry)
                                    if getattr(resp, 'ok', False):
                                        try:
                                            data = resp.json()
                                        except Exception:
                                            data = {"text": resp.text[:400] if hasattr(resp, 'text') else None}
                                        return {"updated": True, "http": resp.status_code, "response": data, "request": body, "path": purl, "method": method, "attempts": tries}
                                    # If 404 for PATCH, try next variant; if 400, continue variants
                                    if getattr(resp, 'status_code', None) == 404:
                                        break
                                except Exception as e:
                                    tries.append({"url": purl, "method": method, "exception": str(e), "body_shape": ["data"], "content_type": "application/vnd.api+json"})
        except Exception:
            pass

        # Note: We intentionally do not POST to adjustments/receive endpoints.
        # Per requirement, updates must use PUT (or PATCH) to the item resource.
        return {"updated": False, "attempts": tries, "error": "no_variant_succeeded", "request": {"quantity": qty_int}}

    def get_inventory_item(self, item_id: str) -> Dict[str, Any]:
        """Fetch a single inventory item by id, trying a few endpoint shapes."""
        if not item_id:
            return {"found": False, "error": "missing_item_id"}
        id_str = str(item_id).strip()
        base = self.base_url.rstrip('/')
        paths = [
            f"{base}/inventory-items/{id_str}",
            f"{base}/api/v2/inventory-items/{id_str}",
            f"{base}/inventory_items/{id_str}",
        ]
        headers = self._headers()
        tries = []
        for url in paths:
            try:
                resp = self.session.get(url, headers=headers, timeout=20)
                tries.append({"url": url, "status": getattr(resp, 'status_code', None), "ok": getattr(resp, 'ok', None)})
                if getattr(resp, 'ok', False):
                    try:
                        data = resp.json()
                    except Exception:
                        data = {"text": resp.text[:400] if hasattr(resp, 'text') else None}
                    item = data if isinstance(data, dict) else {}
                    return {"found": True, "item": item, "http": resp.status_code, "path": url}
                if getattr(resp, 'status_code', None) == 404:
                    continue
            except Exception as e:
                tries.append({"url": url, "exception": str(e)})
        return {"found": False, "attempts": tries, "error": "not_found"}

    def get_order_request(self, order_id: str) -> Dict[str, Any]:
        """Fetch a single order request by id using the documented endpoint.
        Returns {found: bool, order: dict, http: int, path: str} on success.
        """
        if not order_id:
            return {"found": False, "error": "missing_order_id"}
        id_str = str(order_id).strip()
        url = f"{self.base_url.rstrip('/')}/order-requests/{id_str}"
        tries = []
        try:
            resp = self.session.get(url, headers=self._headers(), timeout=20)
            tries.append({"url": url, "status": getattr(resp, 'status_code', None), "ok": getattr(resp, 'ok', None)})
            if getattr(resp, 'ok', False):
                try:
                    data = resp.json() or {}
                except Exception:
                    data = {"text": resp.text[:400] if hasattr(resp, 'text') else None}
                order = data if isinstance(data, dict) else {}
                return {"found": True, "order": order, "http": resp.status_code, "path": url}
            return {"found": False, "http": getattr(resp, 'status_code', None), "attempts": tries}
        except Exception as e:
            tries.append({"url": url, "exception": str(e)})
            return {"found": False, "attempts": tries, "error": str(e)}

    # ---------- Inventory matching (candidates + best) ----------
    @staticmethod
    def _norm(s: Optional[str]) -> str:
        if s is None:
            return ""
        try:
            return " ".join(str(s).strip().lower().split())
        except Exception:
            return ""

    def _extract_inv_catalog(self, it: Dict[str, Any]) -> str:
        fields = [
            "catalog_number",
            "vendor_product_id",
            "vendor_product",
            "sku",
            "item_number",
            "product_number",
        ]
        for k in fields:
            v = it.get(k)
            if v:
                try:
                    v = str(v).strip()
                    if v:
                        return v
                except Exception:
                    continue
        return ""

    # Build a link to view an inventory item in Quartzy app
    def build_quartzy_item_link(self, item_id: str) -> str:
        """Build a direct item link. Prefer non-group route to avoid 404s when group id mismatches.
        Returns plain inventory/items/{id} which typically redirects within the SPA to the correct group context.
        """
        item = str(item_id).strip()
        return f"https://app.quartzy.com/inventory/items/{item}"

    def build_quartzy_item_link_variants(self, item_id: str) -> Dict[str, str]:
        """Return both default and group-scoped item URLs for debugging or fallback display."""
        item = str(item_id).strip()
        default_link = f"https://app.quartzy.com/inventory/items/{item}"
        gid = (
            getattr((getattr(_APP_CONFIG, 'quartzy', None) or {}), 'group_id', None)
            or getattr(_APP_CONFIG.quartzy, 'group_id', None)
            or getattr(_APP_CONFIG.quartzy, 'lab_id', None)
            or self.lab_id
        )
        group_link = f"https://app.quartzy.com/groups/{gid}/inventory/items/{item}" if gid else None
        out = {"default_link": default_link}
        if group_link:
            out["group_link"] = group_link
        return out

    def find_inventory_candidates(self, item_name: Optional[str] = None, vendor_name: Optional[str] = None, catalog_number: Optional[str] = None, lab_id: Optional[str] = None, limit: int = 5) -> Dict[str, Any]:
        items = self.fetch_inventory_items_fast(lab_id)
        q_name = self._norm(item_name)
        q_vendor = self._norm(vendor_name)
        q_cat = self._norm(catalog_number)
        candidates: List[Dict[str, Any]] = []
        for it in items:
            nm = self._norm(it.get("name") or it.get("item_name"))
            vn = self._norm(it.get("vendor") or it.get("vendor_name"))
            cn_raw = self._extract_inv_catalog(it)
            cn = self._norm(cn_raw)
            score = 0
            # Strong signal: exact catalog number match
            if q_cat and cn and q_cat == cn:
                score += 100
            # Partial/contains catalog fallbacks
            elif q_cat and cn and (q_cat in cn or cn in q_cat):
                score += 60
            # Name/vendor exact
            if q_name and nm and q_name == nm:
                score += 40
            if q_vendor and vn and q_vendor == vn:
                score += 20
            # Fuzzy name/vendor
            if q_name and nm:
                r = difflib.SequenceMatcher(a=q_name, b=nm).ratio()
                if r >= 0.92:
                    score += 30
                elif r >= 0.85:
                    score += 20
                elif r >= 0.75:
                    score += 10
            if q_vendor and vn:
                r = difflib.SequenceMatcher(a=q_vendor, b=vn).ratio()
                if r >= 0.92:
                    score += 12
                elif r >= 0.85:
                    score += 8
                elif r >= 0.75:
                    score += 4
            if score <= 0:
                continue
            rec = self._extract_location_record(it)
            candidates.append({
                "item_name": it.get("name") or it.get("item_name"),
                "vendor_name": it.get("vendor") or it.get("vendor_name"),
                "catalog_number": cn_raw,
                "score": score,
                "location": rec.get("location"),
                "sub_location": rec.get("sub_location"),
                "id": it.get("id"),
            })
        candidates.sort(key=lambda x: (-x.get("score", 0), (x.get("item_name") or "")))
        return {
            "query": {"item_name": item_name, "vendor_name": vendor_name, "catalog_number": catalog_number},
            "count": len(candidates),
            "candidates": candidates[: max(1, int(limit))],
        }

    def find_inventory_match_best(self, item_name: Optional[str] = None, vendor_name: Optional[str] = None, catalog_number: Optional[str] = None, lab_id: Optional[str] = None) -> Dict[str, Any]:
        res = self.find_inventory_candidates(item_name=item_name, vendor_name=vendor_name, catalog_number=catalog_number, lab_id=lab_id, limit=1)
        if res.get("candidates"):
            top = res["candidates"][0]
            return {"found": True, "item": top, "location": top.get("location"), "sub_location": top.get("sub_location")}
        return {"found": False}

    # ---------- Fast fetching (parallel pages, local filter) ----------
    STATUS_KEYS = [
        "status",
        "state",
        "workflow_status",
        "workflowState",
        "order_status",
        "request_status",
        "fulfillment_status",
    ]

    def _extract_status(self, it: Dict[str, Any]) -> str:
        for k in self.STATUS_KEYS:
            if k in it and it[k] is not None:
                try:
                    return str(it[k]).strip()
                except Exception:
                    continue
        return ""

    def _list_order_requests_page(self, lab_id: Optional[str], page: int, per_page: int) -> Tuple[List[Dict[str, Any]], bool, Dict[str, Any]]:
        url = f"{self.base_url}/order-requests"
        params: Dict[str, Any] = {"page": page, "per_page": per_page, "limit": per_page}
        if lab_id or self.lab_id:
            params["lab_id"] = str(lab_id or self.lab_id)
        resp = self.session.get(url, headers=self._headers(), params=params, timeout=30)
        if not resp.ok:
            return [], False, {"page": page, "total_pages": None, "eff_page_size": None, "http": resp.status_code}
        data = resp.json() or {}
        if isinstance(data, list):
            items = data
            meta = {}
            links_obj = {}
            pagination_obj = {}
        else:
            items = data.get("order_requests") or data.get("data") or data.get("results") or data.get("items") or []
            meta = data.get("meta") or {}
            links_obj = data.get("links") or data.get("_links") or {}
            pagination_obj = data.get("pagination") or {}
        headers = resp.headers
        has_next = False
        x_page = headers.get("X-Page") or headers.get("Page")
        x_total_pages = headers.get("X-Total-Pages") or headers.get("Total-Pages")
        x_next_page = headers.get("X-Next-Page") or headers.get("Next-Page")
        curr_page = None
        total_pages = None
        try:
            if x_page: curr_page = int(x_page)
            if x_total_pages: total_pages = int(x_total_pages)
        except Exception:
            pass
        if not has_next and x_total_pages and x_page:
            try:
                has_next = int(x_page) < int(x_total_pages)
            except Exception:
                pass
        if not has_next and x_next_page:
            has_next = True
        if not has_next:
            link_hdr = headers.get("Link") or headers.get("link")
            if link_hdr and 'rel="next"' in link_hdr.lower():
                has_next = True
        if not has_next and isinstance(links_obj, dict):
            nxt = links_obj.get("next") or links_obj.get("next_page") or links_obj.get("nextUrl")
            has_next = bool(nxt)
        if not has_next:
            pg_info = {**({} if not isinstance(meta, dict) else meta), **({} if not isinstance(pagination_obj, dict) else pagination_obj)}
            curr = pg_info.get("current_page") or pg_info.get("page") or pg_info.get("page_number")
            pages = pg_info.get("total_pages") or pg_info.get("pages")
            nxt = pg_info.get("next_page") or pg_info.get("next")
            if isinstance(curr, int): curr_page = curr
            if isinstance(pages, int): total_pages = pages
            if not has_next and isinstance(curr, int) and isinstance(pages, int):
                has_next = curr < pages
            if not has_next and nxt is not None:
                has_next = True
        eff_page_size = None
        for k in ("X-Per-Page", "Per-Page", "x-per-page", "x_per_page"):
            if k in headers:
                try:
                    eff_page_size = int(headers[k]); break
                except Exception:
                    pass
        if not eff_page_size and isinstance(meta, dict):
            for k in ("per_page", "page_size", "perPage", "pageSize"):
                if k in meta:
                    try:
                        eff_page_size = int(meta[k]); break
                    except Exception:
                        pass
        if not eff_page_size and isinstance(pagination_obj, dict):
            for k in ("per_page", "page_size"):
                if k in pagination_obj:
                    try:
                        eff_page_size = int(pagination_obj[k]); break
                    except Exception:
                        pass
        if eff_page_size is None:
            eff_page_size = 25
        if total_pages is None and eff_page_size:
            total_count = headers.get("X-Total-Count") or headers.get("Total-Count")
            if total_count:
                try:
                    total_pages = (int(total_count) + eff_page_size - 1) // eff_page_size
                except Exception:
                    pass
        if not has_next:
            has_next = len(items) == eff_page_size
        page_meta = {"page": curr_page or page, "total_pages": total_pages, "eff_page_size": eff_page_size}
        return items, has_next, page_meta

    def _filter_by_status_exact(self, items: List[Dict[str, Any]], statuses: Optional[List[str]]) -> List[Dict[str, Any]]:
        if not statuses:
            return items
        targets = {str(s).strip().upper() for s in statuses if str(s).strip()}
        out: List[Dict[str, Any]] = []
        for it in items:
            if (self._extract_status(it) or "").upper() in targets:
                out.append(it)
        return out

    def fetch_order_requests_fast(self, statuses: Optional[List[str]] = None, lab_id: Optional[str] = None, per_page: Optional[int] = None, max_workers: Optional[int] = None):
        lab = lab_id or self.lab_id or None
        per_page = int(per_page or self.orders_per_page)
        max_workers = int(max_workers or self.max_workers)
        # Page 1 to discover total_pages
        first_items, has_next, meta = self._list_order_requests_page(lab, page=1, per_page=per_page)
        matched = self._filter_by_status_exact(first_items, statuses)
        results: List[Dict[str, Any]] = []
        results.extend(matched)
        total_pages = meta.get("total_pages")
        if not has_next and (not total_pages or total_pages == 1):
            return [self._map_order(o) for o in results], {"pages": 1, "last_status": 200, "eff_page_size": meta.get("eff_page_size")}
        # If unknown total pages, continue serially to avoid gaps
        if not total_pages:
            # Use stride-based windowed parallel scan for maximum throughput
            from concurrent.futures import ThreadPoolExecutor, as_completed
            stride = max(1, int(max_workers))
            start_page = 2
            end_page = 1 + stride
            initial_pages = list(range(start_page, end_page + 1))

            with ThreadPoolExecutor(max_workers=stride) as ex:
                futs = {ex.submit(self._list_order_requests_page, lab, p, per_page): p for p in initial_pages}
                max_page_seen = 1
                while futs:
                    for fut in as_completed(list(futs.keys())):
                        p = futs.pop(fut)
                        try:
                            page_items, _, _ = fut.result()
                        except Exception:
                            page_items = []
                        if page_items:
                            matched = self._filter_by_status_exact(page_items, statuses)
                            results.extend(matched)
                            max_page_seen = max(max_page_seen, p)
                            next_p = p + stride
                            # schedule next stride page
                            futs[ex.submit(self._list_order_requests_page, lab, next_p, per_page)] = next_p
                return [self._map_order(o) for o in results], {"pages": max_page_seen, "last_status": 200, "eff_page_size": meta.get("eff_page_size")}
        # Parallel fetch of remaining pages
        from concurrent.futures import ThreadPoolExecutor, as_completed
        pages = list(range(2, int(total_pages) + 1))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(self._list_order_requests_page, lab, p, per_page): p for p in pages}
            for fut in as_completed(futs):
                p = futs[fut]
                try:
                    page_items, _, _ = fut.result()
                except Exception:
                    continue
                matched = self._filter_by_status_exact(page_items, statuses)
                results.extend(matched)
        return [self._map_order(o) for o in results], {"pages": int(total_pages), "last_status": 200, "eff_page_size": meta.get("eff_page_size")}

    # ---------- a couple of light helpers kept for drop-in behavior ----------
    def list_labs(self, refresh: bool = False):
        if not self.enabled or not self.api_token:
            return []
        if not refresh:
            cached = self._cache_get("labs")
            if cached is not None:
                return cached
        try:
            resp = self.session.get(f"{self.base_url}/labs", headers=self._headers(), timeout=25)
            if not resp.ok:
                return []
            data = resp.json() or []
            labs = data if isinstance(data, list) else data.get("labs") or []
            simple = [{"id": l.get("id"), "name": l.get("name"), "organization_id": (l.get("organization") or {}).get("id")} for l in labs if isinstance(l, dict)]
            self._cache_set("labs", simple)
            return simple
        except Exception:
            return []

    def list_types(self, refresh: bool = False, lab_id: Optional[str] = None):
        if not self.enabled or not self.api_token:
            return []
        if not refresh:
            cached = self._cache_get("types")
            if cached is not None:
                return cached
        try:
            params = {}
            if lab_id or self.lab_id:
                params["lab_id"] = lab_id or self.lab_id
            resp = self.session.get(f"{self.base_url}/types", headers=self._headers(), params=params, timeout=25)
            if not resp.ok:
                return []
            data = resp.json() or []
            types_list = data if isinstance(data, list) else data.get("types") or []
            simple = []
            for t in types_list:
                if not isinstance(t, dict):
                    continue
                lab = t.get("lab") or {}
                simple.append({"id": t.get("id"), "name": t.get("name"), "lab_id": lab.get("id")})
            self._cache_set("types", simple)
            return simple
        except Exception:
            return []

    # inventory search (simple)
    def inventory_search(self, query: str, pages: int = 1):
        qkey = (query or "").strip().lower()
        if not qkey or not self.enabled or not self.api_token:
            return []
        cached = self._cache_get("inventory_search", qkey)
        if cached is not None:
            return cached
        results = []
        for page in range(1, pages + 1):
            try:
                params: Dict[str, Any] = {"page": page}
                if self.lab_id:
                    params["lab_id"] = str(self.lab_id)
                resp = self.session.get(f"{self.base_url}/inventory-items", headers=self._headers(), params=params, timeout=25)
                if not resp.ok:
                    break
                data = resp.json() or []
                items = data if isinstance(data, list) else data.get("inventory_items") or []
                if not items:
                    break
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    nm = (it.get("name") or it.get("item_name") or "").strip()
                    if qkey in nm.lower():
                        results.append({"id": it.get("id"), "name": nm, "quantity": it.get("quantity")})
                if len(results) >= 25:
                    break
            except Exception:
                break
        self._cache_set("inventory_search", results, qkey)
        return results

    # inventory lookup (by code)
    def inventory_lookup(self, code: str, max_pages: Optional[int] = None):
        target = (code or "").strip()
        if not target:
            return {"found": False, "code": code, "reason": "empty_code"}
        if not self.enabled or not self.api_token:
            return {"found": False, "code": code, "reason": "quartzy_disabled_or_token_missing"}
        max_pages = max_pages or self.inventory_lookup_max_pages
        target_lower = target.lower()
        for page in range(1, max_pages + 1):
            try:
                params: Dict[str, Any] = {"page": page}
                if self.lab_id:
                    params["lab_id"] = str(self.lab_id)
                resp = self.session.get(f"{self.base_url}/inventory-items", headers=self._headers(), params=params, timeout=25)
                if not resp.ok:
                    return {"found": False, "code": code, "http": resp.status_code, "body": resp.text[:180], "page": page}
                data = resp.json() or []
                items = data if isinstance(data, list) else data.get("inventory_items") or []
                if not items:
                    break
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    name_val = (it.get("name") or it.get("item_name") or "").strip()
                    catalog_number = (it.get("catalog_number") or "").strip()
                    vendor_product_id = (it.get("vendor_product_id") or it.get("vendor_product") or "").strip()
                    candidates = [name_val.lower(), catalog_number.lower(), vendor_product_id.lower()]
                    if target_lower and target_lower in candidates:
                        normalized = {
                            "item_name": name_val,
                            "vendor_name": it.get("vendor") or it.get("vendor_name") or "",
                            "catalog_number": catalog_number,
                            "quantity": it.get("quantity"),
                            "unit_size": it.get("unit_size"),
                            "unit_price": it.get("unit_price"),
                            "total_price": (it.get("total_price") or {}).get("amount") if isinstance(it.get("total_price"), dict) else None,
                            "location": (it.get("location") or {}).get("name") if isinstance(it.get("location"), dict) else None,
                            "sublocation": (it.get("sublocation") or {}).get("name") if isinstance(it.get("sublocation"), dict) else None,
                            "lot_number": it.get("lot_number"),
                            "cas_number": it.get("cas_number"),
                            "expiration_date": it.get("expiration_date"),
                            "lab_id": (it.get("lab") or {}).get("id") if isinstance(it.get("lab"), dict) else None,
                            "type_id": (it.get("type") or {}).get("id") if isinstance(it.get("type"), dict) else None,
                            "app_url": it.get("app_url"),
                            "raw_id": it.get("id")
                        }
                        return {"found": True, "code": code, "page": page, "item": it, "normalized": normalized}
            except Exception as e:
                return {"found": False, "code": code, "exception": str(e), "page": page}
        return {"found": False, "code": code, "reason": "not_found", "pages_scanned": max_pages}

    # ------- Workaround helpers: ensure item exists or build Add link -------
    def build_quartzy_add_link(self, item_name: str, vendor_name: Optional[str] = None, catalog_number: Optional[str] = None, location: Optional[str] = None, lab_id: Optional[str] = None) -> str:
        """Generate a link to open the Inventory page (reliable). Prefill params are best-effort.
        We use the plain '/inventory' route to avoid SPA 404s seen on '/inventory/new'.
        """
        base = "https://app.quartzy.com/inventory"
        params: Dict[str, Any] = {
            "name": item_name or None,
            "vendor": vendor_name or None,
            "catalog_number": catalog_number or None,
            "location": location or None,
        }
        clean = {k: v for k, v in params.items() if v}
        try:
            from urllib.parse import urlencode
            q = urlencode(clean)
            return f"{base}?{q}" if q else base
        except Exception:
            return base

    def build_quartzy_add_link_variants(self, item_name: str, vendor_name: Optional[str] = None, catalog_number: Optional[str] = None, location: Optional[str] = None, lab_id: Optional[str] = None) -> Dict[str, str]:
        """Return both default and group-scoped inventory URLs for debugging or fallback display."""
        def _encode(params: Dict[str, Any]) -> str:
            try:
                from urllib.parse import urlencode
                return urlencode({k: v for k, v in params.items() if v})
            except Exception:
                return ""
        params = {"name": item_name or None, "vendor": vendor_name or None, "catalog_number": catalog_number or None, "location": location or None}
        q = _encode(params)
        default_link = f"https://app.quartzy.com/inventory{('?' + q) if q else ''}"
        gid = (
            getattr((getattr(_APP_CONFIG, 'quartzy', None) or {}), 'group_id', None)
            or getattr(_APP_CONFIG.quartzy, 'group_id', None)
            or getattr(_APP_CONFIG.quartzy, 'lab_id', None)
            or self.lab_id
        )
        group_link = f"https://app.quartzy.com/groups/{gid}/inventory{('?' + q) if q else ''}" if gid else None
        out = {"default_link": default_link}
        if group_link:
            out["group_link"] = group_link
        return out

    def ensure_item_in_inventory(self, item_name: str, vendor_name: Optional[str] = None, catalog_number: Optional[str] = None, location: Optional[str] = None, lab_id: Optional[str] = None, pages: int = 2) -> Dict[str, Any]:
        """Simple existence check via search; if not found, return a prefilled Add Item link.
        Mirrors the workaround behavior proposed by ChatGPT."""
        nm = (item_name or "").strip()
        if not nm:
            return {"exists": False, "error": "item_name required"}
        try:
            results = self.inventory_search(query=nm, pages=pages) or []
        except Exception:
            results = []
        # naive matching similar to the snippet (substring matches)
        vnorm = (vendor_name or "").strip().lower()
        cnorm = (catalog_number or "").strip().lower()
        for it in results:
            name_ok = nm.lower() in (it.get("name") or "").lower()
            vend_ok = (not vnorm) or (vnorm and vnorm in (it.get("vendor") or it.get("vendor_name") or "").lower())
            cat_ok = (not cnorm) or (cnorm and cnorm in (it.get("catalog_number") or "").lower())
            if name_ok and vend_ok and cat_ok:
                return {"exists": True, "item": it}
        link = self.build_quartzy_add_link(item_name=nm, vendor_name=vendor_name, catalog_number=catalog_number, location=location, lab_id=lab_id)
        return {"exists": False, "add_link": link}

    # ---------- Order Requests: status updates ----------
    def update_order_request_status(self, order_id: str, new_status: str = "RECEIVED") -> Dict[str, Any]:
        """Attempt to update a Quartzy order-request status.
        Tries multiple endpoint variants, body shapes, and status casings.
        Returns a diagnostic payload; does not throw.
        """
        if not order_id:
            return {"updated": False, "error": "missing_order_id"}
        if not self.enabled or not self.api_token:
            return {"updated": False, "error": "quartzy_disabled_or_token_missing"}
        id_str = str(order_id).strip()
        # Endpoint variants observed across API shapes
        base = self.base_url.rstrip('/')
        paths = [
            f"{base}/order-requests/{id_str}",
            f"{base}/api/v2/order-requests/{id_str}",
            f"{base}/order_requests/{id_str}",
            f"{base}/api/v2/order_requests/{id_str}",
        ]
        # Try common status casings
        status_variants = []
        if new_status:
            status_variants = list(dict.fromkeys([new_status, new_status.upper(), new_status.lower(), new_status.capitalize()]))
        else:
            status_variants = ["RECEIVED", "received"]
        tries: List[Dict[str, Any]] = []
        headers = {**self._headers(), "Content-Type": "application/json"}
        for url in paths:
            for st in status_variants:
                bodies = [
                    {"status": st},
                    {"order_request": {"status": st}},
                    {"order_status": st},
                ]
                for body in bodies:
                    # PUT
                    try:
                        resp = self.session.put(url, headers=headers, json=body, timeout=20)
                        err_snip = None
                        try:
                            if not getattr(resp, 'ok', False):
                                err_snip = (resp.text or "")[:240]
                        except Exception:
                            err_snip = None
                        tries.append({"url": url, "method": "PUT", "status": getattr(resp, 'status_code', None), "ok": getattr(resp, 'ok', None), "status_value": st, "body_shape": list(body.keys()), "resp_snippet": err_snip})
                        if getattr(resp, 'ok', False):
                            try:
                                data = resp.json()
                            except Exception:
                                data = {"text": resp.text[:300] if hasattr(resp, 'text') else None}
                            return {"updated": True, "http": resp.status_code, "response": data, "request": body, "path": url}
                        # If 404 on this path, try next path variant immediately
                        if getattr(resp, 'status_code', None) == 404:
                            break
                    except Exception as e:
                        tries.append({"url": url, "method": "PUT", "exception": str(e), "status_value": st, "body_shape": list(body.keys())})
                    # PATCH
                    try:
                        resp = self.session.patch(url, headers=headers, json=body, timeout=20)
                        err_snip = None
                        try:
                            if not getattr(resp, 'ok', False):
                                err_snip = (resp.text or "")[:240]
                        except Exception:
                            err_snip = None
                        tries.append({"url": url, "method": "PATCH", "status": getattr(resp, 'status_code', None), "ok": getattr(resp, 'ok', None), "status_value": st, "body_shape": list(body.keys()), "resp_snippet": err_snip})
                        if getattr(resp, 'ok', False):
                            try:
                                data = resp.json()
                            except Exception:
                                data = {"text": resp.text[:300] if hasattr(resp, 'text') else None}
                            return {"updated": True, "http": resp.status_code, "response": data, "request": body, "path": url}
                        if getattr(resp, 'status_code', None) == 404:
                            break
                    except Exception as e:
                        tries.append({"url": url, "method": "PATCH", "exception": str(e), "status_value": st, "body_shape": list(body.keys())})
        # Per Quartzy API docs, use PUT/PATCH to update order requests; do not POST to action endpoints
        return {"updated": False, "attempts": tries, "request_status": new_status, "error": "no_variant_succeeded"}

    def update_order_request_notes(self, order_id: str, notes: str) -> Dict[str, Any]:
        """Attempt to update a Quartzy order-request notes/comments field via API only.
        Tries multiple endpoint variants and body shapes commonly seen.
        Returns a diagnostic payload similar to status update.
        """
        if not order_id:
            return {"updated": False, "error": "missing_order_id"}
        if notes is None:
            return {"updated": False, "error": "missing_notes"}
        if not self.enabled or not self.api_token:
            return {"updated": False, "error": "quartzy_disabled_or_token_missing"}
        id_str = str(order_id).strip()
        base = self.base_url.rstrip('/')
        paths = [
            f"{base}/order-requests/{id_str}",
            f"{base}/api/v2/order-requests/{id_str}",
            f"{base}/order_requests/{id_str}",
            f"{base}/api/v2/order_requests/{id_str}",
        ]
        # try common field keys
        bodies = [
            {"notes": str(notes)},
            {"order_request": {"notes": str(notes)}},
            {"comment": str(notes)},
            {"comments": str(notes)},
            {"order_request": {"comment": str(notes)}},
        ]
        tries: List[Dict[str, Any]] = []
        headers = {**self._headers(), "Content-Type": "application/json"}
        for url in paths:
            for body in bodies:
                # PUT
                try:
                    resp = self.session.put(url, headers=headers, json=body, timeout=20)
                    err_snip = None
                    try:
                        if not getattr(resp, 'ok', False):
                            err_snip = (resp.text or "")[:240]
                    except Exception:
                        err_snip = None
                    tries.append({"url": url, "method": "PUT", "status": getattr(resp, 'status_code', None), "ok": getattr(resp, 'ok', None), "body_shape": list(body.keys()), "resp_snippet": err_snip})
                    if getattr(resp, 'ok', False):
                        try:
                            data = resp.json()
                        except Exception:
                            data = {"text": resp.text[:300] if hasattr(resp, 'text') else None}
                        return {"updated": True, "http": resp.status_code, "response": data, "request": body, "path": url}
                    if getattr(resp, 'status_code', None) == 404:
                        break
                except Exception as e:
                    tries.append({"url": url, "method": "PUT", "exception": str(e), "body_shape": list(body.keys())})
                # PATCH
                try:
                    resp = self.session.patch(url, headers=headers, json=body, timeout=20)
                    err_snip = None
                    try:
                        if not getattr(resp, 'ok', False):
                            err_snip = (resp.text or "")[:240]
                    except Exception:
                        err_snip = None
                    tries.append({"url": url, "method": "PATCH", "status": getattr(resp, 'status_code', None), "ok": getattr(resp, 'ok', None), "body_shape": list(body.keys()), "resp_snippet": err_snip})
                    if getattr(resp, 'ok', False):
                        try:
                            data = resp.json()
                        except Exception:
                            data = {"text": resp.text[:300] if hasattr(resp, 'text') else None}
                        return {"updated": True, "http": resp.status_code, "response": data, "request": body, "path": url}
                    if getattr(resp, 'status_code', None) == 404:
                        break
                except Exception as e:
                    tries.append({"url": url, "method": "PATCH", "exception": str(e), "body_shape": list(body.keys())})
        return {"updated": False, "attempts": tries, "error": "no_variant_succeeded"}

    # ---------- New simple workflow: strict match then update or open order page ----------
    def _norm_exact(self, s: Optional[str]) -> str:
        try:
            return (s or "").strip().lower()
        except Exception:
            return ""

    def strict_inventory_match(self, item_name: Optional[str], vendor_name: Optional[str], catalog_number: Optional[str], lab_id: Optional[str] = None) -> Dict[str, Any]:
        """Deterministic inventory match:
        - First: exact catalog number (case-insensitive)
        - Else: exact item_name AND vendor_name (both case-insensitive)
        - Else: no match
        Returns {found, item, id}
        """
        items = self.fetch_inventory_items_fast(lab_id)
        nm = self._norm_exact(item_name)
        vn = self._norm_exact(vendor_name)
        cn = self._norm_exact(catalog_number)
        # 1) Exact catalog number
        if cn:
            for it in items:
                cand = self._norm_exact(
                    it.get("catalog_number")
                    or it.get("vendor_product_id")
                    or it.get("vendor_product")
                )
                if cand and cand == cn:
                    return {"found": True, "item": it, "id": it.get("id")}
        # 2) Exact name + vendor
        if nm and vn:
            for it in items:
                iname = self._norm_exact(it.get("name") or it.get("item_name"))
                ivend = self._norm_exact(it.get("vendor") or it.get("vendor_name"))
                if iname == nm and ivend == vn:
                    return {"found": True, "item": it, "id": it.get("id")}
        return {"found": False}

    def receive_order_basic(self, order_id: str, received_quantity: Optional[int] = None, location: Optional[str] = None, sub_location: Optional[str] = None, lab_id: Optional[str] = None) -> Dict[str, Any]:
        """Simple receive workflow per new approach:
        - Fetch order
        - Strict inventory match (catalog number, else name+vendor)
        - If no match => instruct to open order page
        - If match => add received_quantity to current quantity and PUT update; mark order RECEIVED
        """
        if not order_id:
            return {"success": False, "error": "missing_order_id"}
        ord_res = self.get_order_request(str(order_id))
        if not (ord_res and ord_res.get("found")):
            return {"success": False, "error": "order_not_found", "order_id": order_id}
        order = ord_res.get("order") or {}
        app_url = order.get("app_url") or None
        item_name = (order.get("item_name") or order.get("name") or "").strip()
        vendor_name = (order.get("vendor_name") or order.get("vendor") or "").strip()
        catalog_number = None
        try:
            catalog_number, _ = self._extract_catalog_number(order)
        except Exception:
            catalog_number = None
        # Choose received quantity: provided, else order quantity, else 1
        recv_qty = None
        if received_quantity is not None:
            try:
                recv_qty = int(received_quantity)
            except Exception:
                recv_qty = None
        if recv_qty is None:
            oq = order.get("quantity")
            try:
                if oq is not None:
                    recv_qty = int(str(oq).split(".")[0])
            except Exception:
                recv_qty = None
        if recv_qty is None:
            recv_qty = 1
        # Strict match
        match = self.strict_inventory_match(item_name=item_name, vendor_name=vendor_name, catalog_number=catalog_number, lab_id=lab_id)
        if not match.get("found"):
            return {
                "success": False,
                "action": "open_request",
                "reason": "no_inventory_match",
                "order_id": order_id,
                "order_url": app_url,
                "order": {"name": item_name, "vendor": vendor_name, "catalog_number": catalog_number},
            }
        inv_id = match.get("id")
        # Fetch current to compute target
        current_qty = 0
        try:
            curr = self.get_inventory_item(str(inv_id))
            raw = (curr.get("item") or {}) if isinstance(curr, dict) else {}
            # Try common keys
            for k in ("quantity", "quantity_in_stock", "qty", "in_stock_quantity"):
                if k in raw and raw[k] is not None:
                    try:
                        current_qty = int(str(raw[k]).split(".")[0]); break
                    except Exception:
                        pass
        except Exception:
            current_qty = 0
        target_qty = max(0, int(current_qty) + int(recv_qty))
        upd = self.update_inventory_item(item_id=str(inv_id), quantity=str(target_qty))
        # Order status to RECEIVED
        ord_upd = self.update_order_request_status(order_id=str(order_id), new_status="RECEIVED")
        item_url = self.build_quartzy_item_link(str(inv_id)) if inv_id else None
        return {
            "success": bool(upd.get("updated") and (ord_upd.get("updated") or ord_upd.get("success"))),
            "inventory": {**upd, "item_id": inv_id, "item_url": item_url, "target_quantity": target_qty, "added": recv_qty, "current": current_qty},
            "order": {**ord_upd, "order_id": order_id, "order_url": app_url},
        }

    # collect unique lab locations and sub-locations by scanning inventory
    def collect_lab_locations(self, lab_id: Optional[str] = None, pages: int = 5, refresh: bool = False):
        if not self.enabled or not self.api_token:
            return {"locations": [], "sub_locations": [], "sample_size": 0, "disabled": True}
        cache_key = f"lab_locations:{lab_id or self.lab_id}:{pages}"
        if not refresh:
            cached = self._cache_get(cache_key)
            if cached is not None:
                return cached
        lab = lab_id or self.lab_id or None
        loc_set = set()
        sub_set = set()
        scanned = 0
        for page in range(1, max(1, pages) + 1):
            try:
                params: Dict[str, Any] = {"page": page}
                if lab:
                    params["lab_id"] = str(lab)
                resp = self.session.get(f"{self.base_url}/inventory-items", headers=self._headers(), params=params, timeout=25)
                if not resp.ok:
                    break
                data = resp.json() or []
                items = data if isinstance(data, list) else data.get("inventory_items") or []
                if not items:
                    break
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    scanned += 1
                    loc = (it.get("location") or {}).get("name") if isinstance(it.get("location"), dict) else None
                    sub = (it.get("sublocation") or {}).get("name") if isinstance(it.get("sublocation"), dict) else None
                    if isinstance(loc, str) and loc.strip():
                        loc_set.add(loc.strip())
                    if isinstance(sub, str) and sub.strip():
                        sub_set.add(sub.strip())
            except Exception:
                break
        result = {"locations": sorted(loc_set), "sub_locations": sorted(sub_set), "sample_size": scanned}
        self._cache_set(cache_key, result)
        return result

quartzy_service = QuartzyService()

# ---- Backward-compatible free-function wrappers (for legacy imports/tests) ----
def quartzy_fetch_order_requests(statuses: List[str] | None = None, lab_id: Optional[str] = None, all_pages: bool = True, page_limit: Optional[int] = None, safety_max: int = 200):
    return quartzy_service.fetch_order_requests(statuses=statuses, lab_id=lab_id, all_pages=all_pages, page_limit=page_limit, safety_max=safety_max)

def fetch_order_requests(statuses: List[str] | None = None, lab_id: Optional[str] = None, all_pages: bool = True, page_limit: Optional[int] = None, safety_max: int = 200):
    # Legacy alias used by some imports
    return quartzy_service.fetch_order_requests(statuses=statuses, lab_id=lab_id, all_pages=all_pages, page_limit=page_limit, safety_max=safety_max)

def quartzy_fetch_order_requests_fast(statuses: List[str] | None = None, lab_id: Optional[str] = None, per_page: Optional[int] = None, max_workers: Optional[int] = None):
    return quartzy_service.fetch_order_requests_fast(statuses=statuses, lab_id=lab_id, per_page=per_page, max_workers=max_workers)

def quartzy_list_labs(refresh: bool = False):
    return quartzy_service.list_labs(refresh=refresh)

def quartzy_list_types(refresh: bool = False, lab_id: Optional[str] = None):
    return quartzy_service.list_types(refresh=refresh, lab_id=lab_id)

def quartzy_inventory_search(query: str, pages: int = 1):
    return quartzy_service.inventory_search(query=query, pages=pages)

def quartzy_inventory_lookup(code: str, max_pages: Optional[int] = None):
    return quartzy_service.inventory_lookup(code=code, max_pages=max_pages)

def quartzy_headers(mode: Optional[str] = None) -> dict:
    return quartzy_service._headers(mode)

# Minimal placeholders for legacy functions referenced elsewhere; keep imports working
def quartzy_collect_lab_locations(*args, **kwargs):  # pragma: no cover
    lab_id = kwargs.get("lab_id") or (args[0] if args else None)
    per_page = kwargs.get("per_page")
    refresh = kwargs.get("refresh", False)
    max_workers = kwargs.get("max_workers")
    return quartzy_service.collect_lab_locations_fast(lab_id=lab_id, per_page=per_page, refresh=refresh, max_workers=max_workers)

def quartzy_find_inventory_match(payload: Dict[str, Any] | None = None, **kwargs):  # pragma: no cover
    payload = payload or {}
    return quartzy_service.find_inventory_match(
        item_name=payload.get("item_name") or kwargs.get("item_name"),
        vendor_name=payload.get("vendor_name") or kwargs.get("vendor_name"),
        catalog_number=payload.get("catalog_number") or kwargs.get("catalog_number"),
        lab_id=payload.get("lab_id") or kwargs.get("lab_id"),
    )

def quartzy_update_inventory(payload: Dict[str, Any] | None = None, **kwargs):  # pragma: no cover
    payload = payload or {}
    if kwargs:
        payload = {**payload, **kwargs}
    if not payload.get("item_name"):
        return {"updated": False, "error": "item_name required"}
    res = quartzy_service.create_or_update_inventory(
        item_name=str(payload.get("item_name")),
        vendor_name=payload.get("vendor_name"),
        catalog_number=payload.get("catalog_number"),
        quantity=payload.get("quantity"),
        location=payload.get("location"),
        sub_location=payload.get("sub_location"),
        lab_id=payload.get("lab_id"),
        if_exists=str(payload.get("if_exists") or "update"),
    )
    if res.get("created") or res.get("updated"):
        return {"updated": True, "result": res}
    # Workaround: append to local CSV queue for manual import when creation failed and no UI fallback is requested here
    try:
        fields = {
            "item_name": str(payload.get("item_name", "")),
            "vendor_name": payload.get("vendor_name", ""),
            "catalog_number": payload.get("catalog_number", ""),
            "quantity": payload.get("quantity", ""),
            "location": payload.get("location", ""),
            "sub_location": payload.get("sub_location", ""),
            "lab_id": payload.get("lab_id", ""),
            "requested_at": payload.get("requested_at", ""),
            "requested_by": payload.get("requested_by", ""),
            "notes": payload.get("notes", ""),
        }
        # Default queue path aligns with API router's default
        queue_path = "quartzy_inventory_pending.csv"
        row = quartzy_append_pending_inventory(queue_path, fields)
        return {"updated": False, "queued": True, "queue_file": queue_path, "row": row, "result": res}
    except Exception as e:
        return {"updated": False, "queued": False, "queue_error": str(e), "result": res}

def quartzy_create_inventory(payload: Dict[str, Any] | None = None, **kwargs):  # pragma: no cover
    """Explicit create-or-update wrapper preferred by API router.
    Returns structure with created/updated flags and detailed result.
    """
    payload = payload or {}
    if kwargs:
        payload = {**payload, **kwargs}
    if not payload.get("item_name"):
        return {"created": False, "error": "item_name required"}
    return quartzy_service.create_or_update_inventory(
        item_name=str(payload.get("item_name")),
        vendor_name=payload.get("vendor_name"),
        catalog_number=payload.get("catalog_number"),
        quantity=payload.get("quantity"),
        location=payload.get("location"),
        sub_location=payload.get("sub_location"),
        lab_id=payload.get("lab_id"),
        if_exists=str(payload.get("if_exists") or "update"),
        allow_api_create=kwargs.get("allow_api_create"),
    )
    
def quartzy_update_order_status(order_id: str, new_status: str = "RECEIVED"):  # pragma: no cover
    return quartzy_service.update_order_request_status(order_id=order_id, new_status=new_status)

def quartzy_decide_status(*args, **kwargs):  # pragma: no cover
    return "unknown"

# New wrapper for simple workflow
def quartzy_receive_order_basic(payload: Dict[str, Any] | None = None, **kwargs):  # pragma: no cover
    payload = payload or {}
    if kwargs:
        payload = {**payload, **kwargs}
    order_id = payload.get("order_id") or payload.get("id")
    if not order_id:
        return {"success": False, "error": "missing_order_id"}
    received_quantity = payload.get("received_quantity") or payload.get("quantity")
    location = payload.get("location") or payload.get("storage_location")
    sub_location = payload.get("sub_location")
    lab_id = payload.get("lab_id")
    try:
        rq = quartzy_service.receive_order_basic(order_id=str(order_id), received_quantity=(int(received_quantity) if received_quantity is not None else None), location=location, sub_location=sub_location, lab_id=lab_id)
        return rq
    except Exception as e:
        return {"success": False, "error": str(e)}

# Expose debug dict under legacy name
_quartzy_debug = quartzy_service.debug

# ------- Pending inventory CSV queue helpers (workaround when API cannot create) -------
PENDING_HEADERS = [
    "item_name",
    "vendor_name",
    "catalog_number",
    "quantity",
    "location",
    "sub_location",
    "lab_id",
    "requested_at",
    "requested_by",
    "notes",
]

def quartzy_ensure_pending_inventory_csv(path: str):
    # Create CSV with headers if not present
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(PENDING_HEADERS)

def quartzy_append_pending_inventory(path: str, fields: Dict[str, Any]) -> List[str]:
    quartzy_ensure_pending_inventory_csv(path)
    row = [str(fields.get(h, "") or "") for h in PENDING_HEADERS]
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)
    return row

def quartzy_get_inventory_item(item_id: str):  # pragma: no cover
    return quartzy_service.get_inventory_item(item_id)
