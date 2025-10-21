# src/raw/load_attendance.py
from __future__ import annotations

import argparse
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List

from ..api.mojo_client import MojoApiClient
from ..settings import CONFIG
from .base_loader import insert_attendance_rows, upsert_sync_state
from .common import ensure_attendance_partitions, json_source_hash

ENDPOINT = "/attendance"


def _daterange(d0: date, d1: date) -> Iterable[date]:
    cur = d0
    while cur <= d1:
        yield cur
        cur += timedelta(days=1)


def fetch_attendance(
    client: MojoApiClient, d_from: date, d_to: date
) -> List[Dict[str, Any]]:
    """
    Берём все attendance за период [d_from..d_to].
    Клиент уже слайсит по дням и дедуплицирует по id.
    """
    items = client.attendance_all(d_from.isoformat(), d_to.isoformat())
    return items


def to_raw_rows(
    items: List[Dict[str, Any]], src_day: date, batch_id: str
) -> List[Dict[str, Any]]:
    """
    Приводим элементы к колонкам raw.attendance.
    Не теряем исходник: кладём целиком в raw_json.
    """
    rows: List[Dict[str, Any]] = []
    for it in items:
        # поля из примера /attendance
        att_date = it.get("attendance_date") or it.get("date")
        if isinstance(att_date, str):
            att_date = date.fromisoformat(att_date)

        raw = dict(it)  # полный слепок
        row = {
            "id": it.get("id"),
            "student_id": it.get("student_id"),
            "lesson_id": it.get("lesson_id"),
            "student": it.get("student"),
            "grade": it.get("grade"),
            "attendance_date": att_date,
            "status": it.get("status"),
            "period_name": it.get("period_name"),
            "subject_name": it.get("subject_name"),
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

    items = fetch_attendance(client, d_from, d_to)
    rows = to_raw_rows(items, src_day=date.today(), batch_id=batch_id)

    # партиции (по датам уроков)
    ensure_attendance_partitions([r["attendance_date"] for r in rows])

    inserted = insert_attendance_rows(rows)
    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=d_from,
        window_to=d_to,
        last_seen_updated_at=datetime.now(),
        params={"mode": "init", "inserted": inserted, "batch_id": batch_id},
        notes="init load attendance",
    )
    print(f"[attendance:init] {inserted} rows inserted, window {d_from}..{d_to}")


def run_daily() -> None:
    client = MojoApiClient()
    batch_id = str(uuid.uuid4())

    # окно из конфига (fallback = 2)
    days_back = CONFIG.get("api", {}).get("windows", {}).get("attendance_days_back", 2)
    today = date.today()
    d_from = today - timedelta(days=int(days_back))
    d_to = today

    items = fetch_attendance(client, d_from, d_to)
    rows = to_raw_rows(items, src_day=today, batch_id=batch_id)

    ensure_attendance_partitions([r["attendance_date"] for r in rows])

    inserted = insert_attendance_rows(rows)
    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=d_from,
        window_to=d_to,
        last_seen_updated_at=datetime.now(),
        params={"mode": "daily", "inserted": inserted, "batch_id": batch_id},
        notes="daily window load",
    )
    print(f"[attendance:daily] {inserted} rows inserted, window {d_from}..{d_to}")


def run_backfill(days: List[date]) -> None:
    client = MojoApiClient()
    batch_id = str(uuid.uuid4())

    all_rows: List[Dict[str, Any]] = []
    d_min, d_max = min(days), max(days)

    # тянем точечно, но клиент сам слайсит по дням при необходимости
    items = fetch_attendance(client, d_min, d_max)

    # фильтр только по нужным датам (если важно строго по списку)
    wanted = set(days)
    items = [
        it
        for it in items
        if it.get("attendance_date") in {d.isoformat() for d in wanted}
    ]

    rows = to_raw_rows(items, src_day=date.today(), batch_id=batch_id)
    ensure_attendance_partitions([r["attendance_date"] for r in rows])

    inserted = insert_attendance_rows(rows)
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
        f"[attendance:backfill] {inserted} rows inserted, days={','.join(sorted(d.isoformat() for d in days))}"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RAW loader for /attendance")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--init", action="store_true", help="initial load for a date range")
    g.add_argument("--daily", action="store_true", help="daily sliding window load")
    g.add_argument("--backfill", action="store_true", help="backfill specific days")

    p.add_argument("--from", dest="date_from", type=str, help="YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", type=str, help="YYYY-MM-DD")
    p.add_argument("--days", type=str, help="comma-separated YYYY-MM-DD")

    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.init:
        if not (args.date_from and args.date_to):
            raise SystemExit("--init requires --from and --to")
        d_from = date.fromisoformat(args.date_from)
        d_to = date.fromisoformat(args.date_to)
        run_init(d_from, d_to)
    elif args.daily:
        run_daily()
    elif args.backfill:
        if not args.days:
            raise SystemExit("--backfill requires --days YYYY-MM-DD,YYYY-MM-DD")
        days = [
            date.fromisoformat(s.strip()) for s in args.days.split(",") if s.strip()
        ]
        run_backfill(days)


if __name__ == "__main__":
    main()
