# src/raw/load_schedule.py
from __future__ import annotations

import argparse
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List

from ..api.mojo_client import MojoApiClient
from ..db import get_conn
from ..settings import CONFIG
from .base_loader import insert_schedule_lessons_rows
from .common import json_source_hash

ENDPOINT = "/schedule"


def monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())  # 0=Monday


def week_range(d: date) -> tuple[date, date]:
    start = monday_of(d)
    return start, start + timedelta(days=6)


def ensure_schedule_partitions(dates: Iterable[date]) -> None:
    months = {d.replace(day=1) for d in dates if d}
    if not months:
        return
    with get_conn() as conn, conn.cursor() as cur:
        for m in sorted(months):
            cur.execute("SELECT raw.ensure_schedule_lessons_partition(%s);", (m,))
        conn.commit()


def fetch_schedule_week(
    client: MojoApiClient, any_day_in_week: date
) -> List[Dict[str, Any]]:
    # API отдаёт неделю вокруг search_date
    data = client.schedule(
        search_date=any_day_in_week.isoformat(), limit=client.st.default_limit
    )
    items = data.get("data", {}).get("items", []) or []
    return list(items)


def normalize_items(
    items: List[Dict[str, Any]], src_day: date, batch_id: str
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items:
        # пропустим странные записи без даты/lesson_id
        if not it.get("lesson_id") or not it.get("lesson_date"):
            continue

        # простые преобразования типов
        lesson_date = date.fromisoformat(it["lesson_date"])
        schedule_start = it.get("schedule_start")
        schedule_finish = it.get("schedule_finish")
        if isinstance(schedule_start, str) and schedule_start:
            schedule_start = date.fromisoformat(schedule_start)
        else:
            schedule_start = None
        if isinstance(schedule_finish, str) and schedule_finish:
            schedule_finish = date.fromisoformat(schedule_finish)
        else:
            schedule_finish = None

        staff = it.get("staff") or {}

        row = {
            "schedule_id": it.get("schedule_id"),
            "schedule_start": schedule_start,
            "schedule_finish": schedule_finish,
            "group_id": it.get("group_id"),
            "building_id": it.get("building_id"),
            "group_name": it.get("group"),
            "subject_name": it.get("subject"),
            "room": it.get("room"),
            "is_replacement": it.get("is_replacement"),
            "replaced_schedule_id": it.get("replaced_schedule_id"),
            "lesson_id": it.get("lesson_id"),
            "lesson_date": lesson_date,
            "day_number": it.get("day_number"),
            "lesson_start": it.get("lesson_start"),
            "lesson_finish": it.get("lesson_finish"),
            "staff_json": staff,
            "src_day": src_day,
            "source_system": "mojo",
            "endpoint": ENDPOINT,
            "raw_json": dict(it),
            "ingested_at": datetime.now(),
            "source_hash": json_source_hash(it),
            "batch_id": batch_id,
        }
        out.append(row)
    return out


def run_init(d_from: date, d_to: date) -> None:
    client = MojoApiClient()
    batch_id = str(uuid.uuid4())

    # идём по неделям (понедельники)
    cur = monday_of(d_from)
    end = monday_of(d_to)
    all_rows: List[Dict[str, Any]] = []

    while cur <= end:
        items = fetch_schedule_week(client, cur)
        rows = normalize_items(items, src_day=date.today(), batch_id=batch_id)
        all_rows.extend(rows)
        cur += timedelta(days=7)

    ensure_schedule_partitions([r["lesson_date"] for r in all_rows])
    inserted = insert_schedule_lessons_rows(all_rows)

    start_w, end_w = week_range(d_from)
    start_w2, end_w2 = week_range(d_to)
    from ..raw.base_loader import upsert_sync_state

    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=start_w,
        window_to=end_w2,
        last_seen_updated_at=datetime.now(),
        params={
            "mode": "init",
            "weeks": "by_mondays",
            "inserted": inserted,
            "batch_id": batch_id,
        },
        notes="init schedule by weeks",
    )
    print(f"[schedule:init] {inserted} rows, weeks {start_w}..{end_w2}")


def run_daily() -> None:
    client = MojoApiClient()
    batch_id = str(uuid.uuid4())

    today = date.today()
    start_w, end_w = week_range(today)

    # FETCH
    items = fetch_schedule_week(client, today)
    print(f"[schedule][fetch] week={start_w}..{end_w} fetched={len(items)}")
    if items:
        sample_ids = [str(x.get("lesson_id")) for x in items[:5]]
        distinct_lessons = len(
            {x.get("lesson_id") for x in items if x.get("lesson_id") is not None}
        )
        print(
            f"[schedule][fetch] week={start_w}..{end_w} sample_lesson_id={','.join(sample_ids)} distinct_lesson_id={distinct_lessons}"
        )

    # NORMALIZE
    rows = normalize_items(items, src_day=today, batch_id=batch_id)
    print(
        f"[schedule][normalize] week={start_w}..{end_w} normalized={len(rows)} dropped={len(items) - len(rows)}"
    )

    # INSERT
    ensure_schedule_partitions([r["lesson_date"] for r in rows])
    to_insert = len(rows)
    inserted = insert_schedule_lessons_rows(rows)
    print(
        f"[schedule][insert] week={start_w}..{end_w} to_insert={to_insert} inserted={inserted}"
    )

    from ..raw.base_loader import upsert_sync_state

    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=start_w,
        window_to=end_w,
        last_seen_updated_at=datetime.now(),
        params={"mode": "daily", "inserted": inserted, "batch_id": batch_id},
        notes="daily week load",
    )
    print(f"[schedule:daily] {inserted} rows, week {start_w}..{end_w}")


def run_backfill(mondays: List[date]) -> None:
    client = MojoApiClient()
    batch_id = str(uuid.uuid4())

    uniq_mondays = sorted({monday_of(d) for d in mondays})
    all_rows: List[Dict[str, Any]] = []

    for monday in uniq_mondays:
        start_w, end_w = week_range(monday)

        # FETCH
        items = fetch_schedule_week(client, monday)
        print(f"[schedule][fetch] week={start_w}..{end_w} fetched={len(items)}")
        if items:
            sample_ids = [str(x.get("lesson_id")) for x in items[:5]]
            distinct_lessons = len(
                {x.get("lesson_id") for x in items if x.get("lesson_id") is not None}
            )
            print(
                f"[schedule][fetch] week={start_w}..{end_w} sample_lesson_id={','.join(sample_ids)} distinct_lesson_id={distinct_lessons}"
            )

        # NORMALIZE
        rows = normalize_items(items, src_day=date.today(), batch_id=batch_id)
        print(
            f"[schedule][normalize] week={start_w}..{end_w} normalized={len(rows)} dropped={len(items) - len(rows)}"
        )

        all_rows.extend(rows)

    # INSERT (итогом)
    ensure_schedule_partitions([r["lesson_date"] for r in all_rows])
    to_insert = len(all_rows)
    inserted = insert_schedule_lessons_rows(all_rows)
    print(
        f"[schedule][insert] weeks={','.join(m.isoformat() for m in uniq_mondays)} to_insert={to_insert} inserted={inserted}"
    )

    from ..raw.base_loader import upsert_sync_state

    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=min(mondays) if mondays else None,
        window_to=max(mondays) if mondays else None,
        last_seen_updated_at=datetime.now(),
        params={
            "mode": "backfill",
            "weeks": [m.isoformat() for m in uniq_mondays],
            "inserted": inserted,
            "batch_id": batch_id,
        },
        notes="backfill weeks",
    )
    print(
        f"[schedule:backfill] {inserted} rows, weeks={','.join(m.isoformat() for m in uniq_mondays)}"
    )


def parse_args():
    p = argparse.ArgumentParser(description="RAW loader for /schedule")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--init", action="store_true")
    g.add_argument("--daily", action="store_true")
    g.add_argument("--backfill", action="store_true")
    p.add_argument("--from", dest="date_from", type=str)
    p.add_argument("--to", dest="date_to", type=str)
    p.add_argument(
        "--weeks", type=str, help="comma-separated dates within weeks (YYYY-MM-DD)"
    )
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
        if not args.weeks:
            raise SystemExit(
                "--backfill requires --weeks=YYYY-MM-DD,YYYY-MM-DD (any day in week)"
            )
        days = [
            date.fromisoformat(s.strip()) for s in args.weeks.split(",") if s.strip()
        ]
        run_backfill(days)


if __name__ == "__main__":
    main()
