# src/raw/load_work_forms.py
from __future__ import annotations

import argparse
import uuid
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from ..api.mojo_client import MojoApiClient
from ..settings import CONFIG
from .base_loader import insert_work_forms_rows, upsert_sync_state
from .common import json_source_hash

ENDPOINT = "/work_forms"


def fetch_work_forms(
    client: MojoApiClient, department: Optional[int] = None
) -> List[Dict[str, Any]]:
    data = client.work_forms(department=department)
    # по описанию: data.form_list
    items = data.get("data", {}).get("form_list", []) or []
    return list(items)


def to_raw_rows(
    items: List[Dict[str, Any]], src_day: date, batch_id: str
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in items:
        raw = dict(it)

        # мягкий парс дат (оставим строку как есть, если формат «кривой»)
        def parse_ts(v):
            if isinstance(v, str) and v.strip():
                try:
                    return datetime.fromisoformat(
                        v.replace(" ", "T").replace("Z", "+00:00")
                    )
                except Exception:
                    return v
            return None

        row = {
            "id_form": it.get("id_form"),
            "form_name": it.get("form_name"),
            "form_description": it.get("form_description"),
            "form_area": it.get("form_area"),
            "form_control": it.get("form_control"),
            "form_weight": it.get("form_weight"),
            "form_percent": it.get("form_percent"),
            "form_created": parse_ts(it.get("form_created")),
            "form_archived": parse_ts(it.get("form_archived")),
            "form_deleted": parse_ts(it.get("form_deleted")),
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

    # если когда-нибудь появятся несколько департаментов — можно пройтись циклом.
    items = fetch_work_forms(
        client
    )  # department берётся из настроек клиента (по умолчанию 0)
    rows = to_raw_rows(items, src_day=today, batch_id=batch_id)
    inserted = insert_work_forms_rows(rows)

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
        notes=f"{mode} load work_forms (full snapshot upsert)",
    )
    print(f"[work_forms:{mode}] upserted {inserted} rows, api_items={len(items)}")


def parse_args():
    p = argparse.ArgumentParser(description="RAW loader for /work_forms")
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
