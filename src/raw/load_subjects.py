# src/raw/load_subjects.py
from __future__ import annotations

import argparse
import uuid
from datetime import date, datetime
from typing import Any, Dict, List

from ..api.mojo_client import MojoApiClient
from ..db import get_conn
from ..settings import CONFIG
from .base_loader import insert_subjects_rows, upsert_sync_state
from .common import json_source_hash

ENDPOINT = "/subjects"


def fetch_subjects(client: MojoApiClient) -> List[Dict[str, Any]]:
    data = client.subjects()
    items = data.get("data") or data.get("data", {}).get("items") or []
    # нормализуем к списку
    if isinstance(items, dict):
        items = items.get("items", [])
    return list(items or [])


def to_raw_rows(
    items: List[Dict[str, Any]], src_day: date, batch_id: str
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in items:
        raw = dict(it)
        row = {
            "id": it.get("id"),
            "title": it.get("title"),
            "in_curriculum": it.get("in_curriculum"),
            "in_olymp": it.get("in_olymp"),
            "department": it.get("department"),
            "closed": it.get("closed"),
            "first_seen_src_day": src_day,
            "last_seen_src_day": src_day,
            "src_day": src_day,
            "source_system": "mojo",
            "endpoint": ENDPOINT,
            "raw_json": raw,
            "ingested_at": datetime.now(),
            "source_hash": json_source_hash(raw),
            "batch_id": batch_id,
        }
        rows.append(row)
    return rows


def run_load(mode: str) -> None:
    client = MojoApiClient()
    batch_id = str(uuid.uuid4())
    today = date.today()

    items = fetch_subjects(client)
    rows = to_raw_rows(items, src_day=today, batch_id=batch_id)
    inserted = insert_subjects_rows(rows)

    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=None,
        window_to=None,
        last_seen_updated_at=datetime.now(),
        params={
            "mode": mode,
            "inserted": inserted,
            "batch_id": batch_id,
            "count_api": len(items),
        },
        notes=f"{mode} load subjects (full snapshot upsert)",
    )
    print(f"[subjects:{mode}] upserted {inserted} rows, api_items={len(items)}")


def parse_args():
    p = argparse.ArgumentParser(description="RAW loader for /subjects")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--init", action="store_true")
    g.add_argument("--daily", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    if args.init:
        run_load("init")
    elif args.daily:
        run_load("daily")


if __name__ == "__main__":
    main()
