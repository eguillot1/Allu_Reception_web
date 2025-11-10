"""Storage layer for CSV + SQLite (Phase 2d).
Pure functions; mirrors logic from server_3.py
"""
from __future__ import annotations
import os
import csv
import sqlite3
from typing import List

HEADERS = [
    "timestamp","item_name","quantity","supplier","item_type","bsl2_status","status",
    "storage_location","location","sub_location","received_by","received_by_id",
    "project_manager","comments","pictures"
]
CSV_PATH = "reception_log.csv"
DB_PATH = "reception_log.db"

def ensure_csv_with_header():
    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADERS)

def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS receptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        item_name TEXT,
        quantity TEXT,
        supplier TEXT,
        item_type TEXT,
        project_manager TEXT,
        bsl2_status TEXT,
        status TEXT,
        storage_location TEXT,
        location TEXT,
        sub_location TEXT,
        received_by TEXT,
        received_by_id TEXT,
        comments TEXT,
        pictures TEXT
    )''')
    conn.commit()
    conn.close()

def log_reception_to_db(row: List[str]):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''INSERT INTO receptions (
        timestamp, item_name, quantity, supplier, item_type, project_manager, bsl2_status, status,
        storage_location, location, sub_location, received_by, received_by_id, comments, pictures
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', row)
    conn.commit()
    conn.close()

__all__ = ["HEADERS","CSV_PATH","DB_PATH","ensure_csv_with_header","ensure_db","log_reception_to_db"]
