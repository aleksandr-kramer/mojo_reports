# src/raw/load_marks_current.py
from __future__ import annotations

import argparse
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List

from ..api.mojo_client import MojoApiClient
from ..db import get_conn
from ..settings import CONFIG
from .base_loader import insert_marks_current_rows, upsert_sync_state
from .common import (  # переиспользуем month utils
    ensure_attendance_partitions,
    json_source_hash,
)

ENDPOINT = "/marks/current"


def ensure_marks_partitions(dates):
    # своя функция для /marks/current
    months = {d.replace(day=1) for d in dates if d}
    if not months:
        return
    with get_conn() as conn, conn.cursor() as cur:
        for m in sorted(months):
            cur.execute("SELECT raw.ensure_marks_current_partition(%s);", (m,))
        conn.commit()


def fetch_marks(
    client: MojoApiClient, d_from: date, d_to: date
) -> List[Dict[str, Any]]:
    # клиент уже умеет marks_current_all с посуточным обходом + дедуп по id
    return client.marks_current_all(d_from.isoformat(), d_to.isoformat())


def to_raw_rows(
    items: List[Dict[str, Any]], src_day: date, batch_id: str
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in items:
        md = it.get("date")
        md = date.fromisoformat(md) if isinstance(md, str) else md
        raw = dict(it)
        row = {
            "id": it.get("id"),
            "period": it.get("period"),
            "mark_date": md,
            "subject": it.get("subject"),
            "group_name": it.get("group_name"),
            "id_student": it.get("id_student"),
            "value": it.get("value"),
            "created": it.get("created"),
            "assesment": it.get("assesment"),
            "control": it.get("control"),
            "flex": it.get("flex"),
            "weight": it.get("weight"),
            "form": it.get("form"),
            "grade": it.get("grade"),
            "student": it.get("student"),
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


def run_init(d_from: date, d_to: date) -> None:
    client = MojoApiClient()
    batch_id = str(uuid.uuid4())

    items = fetch_marks(client, d_from, d_to)
    rows = to_raw_rows(items, src_day=date.today(), batch_id=batch_id)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM raw.marks_current WHERE mark_date BETWEEN %s AND %s",
            (d_from, d_to),
        )
        conn.commit()

    ensure_marks_partitions([r["mark_date"] for r in rows])
    inserted = insert_marks_current_rows(rows)

    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=d_from,
        window_to=d_to,
        last_seen_updated_at=datetime.now(),
        params={"mode": "init", "inserted": inserted, "batch_id": batch_id},
        notes="init load marks_current",
    )
    print(f"[marks_current:init] {inserted} rows, window {d_from}..{d_to}")


def run_daily() -> None:
    client = MojoApiClient()
    batch_id = str(uuid.uuid4())

    days_back = CONFIG.get("api", {}).get("windows", {}).get("attendance_days_back", 2)
    today = date.today()
    d_from = today - timedelta(days=int(days_back))
    d_to = today

    items = fetch_marks(client, d_from, d_to)
    rows = to_raw_rows(items, src_day=today, batch_id=batch_id)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM raw.marks_current WHERE mark_date BETWEEN %s AND %s",
            (d_from, d_to),
        )
        conn.commit()

    ensure_marks_partitions([r["mark_date"] for r in rows])
    inserted = insert_marks_current_rows(rows)

    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=d_from,
        window_to=d_to,
        last_seen_updated_at=datetime.now(),
        params={"mode": "daily", "inserted": inserted, "batch_id": batch_id},
        notes="daily window load",
    )
    print(f"[marks_current:daily] {inserted} rows, window {d_from}..{d_to}")


def run_backfill(days: List[date]) -> None:
    client = MojoApiClient()
    batch_id = str(uuid.uuid4())

    d_min, d_max = min(days), max(days)
    items = fetch_marks(client, d_min, d_max)
    wanted = {d.isoformat() for d in days}
    items = [it for it in items if it.get("date") in wanted]

    rows = to_raw_rows(items, src_day=date.today(), batch_id=batch_id)
    ensure_marks_partitions([r["mark_date"] for r in rows])
    # Для backfill удаляем за минимально-максимальный диапазон этих дней
    d_from, d_to = min(days), max(days)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM raw.marks_current WHERE mark_date BETWEEN %s AND %s",
            (d_from, d_to),
        )
        conn.commit()

    inserted = insert_marks_current_rows(rows)

    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=d_min,
        window_to=d_max,
        last_seen_updated_at=datetime.now(),
        params={
            "mode": "backfill",
            "inserted": inserted,
            "batch_id": batch_id,
            "days": [d.isoformat() for d in days],
        },
        notes="backfill",
    )
    print(
        f"[marks_current:backfill] {inserted} rows, days={','.join(sorted(d.isoformat() for d in days))}"
    )


def parse_args():
    p = argparse.ArgumentParser(description="RAW loader for /marks/current")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--init", action="store_true")
    g.add_argument("--daily", action="store_true")
    g.add_argument("--backfill", action="store_true")
    p.add_argument("--from", dest="date_from", type=str)
    p.add_argument("--to", dest="date_to", type=str)
    p.add_argument("--days", type=str)
    return p.parse_args()


def main():
    args = parse_args()
    if args.init:
        if not (args.date_from and args.date_to):
            raise SystemExit("--init requires --from and --to")
        run_init(date.fromisoformat(args.date_from), date.fromisoformat(args.date_to))
    elif args.daily:
        run_daily()
    elif args.backfill:
        if not args.days:
            raise SystemExit("--backfill requires --days")
        days = [
            date.fromisoformat(s.strip()) for s in args.days.split(",") if s.strip()
        ]
        run_backfill(days)


if __name__ == "__main__":
    main()
