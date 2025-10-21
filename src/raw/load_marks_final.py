# src/raw/load_marks_final.py
from __future__ import annotations

import argparse
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from ..api.mojo_client import MojoApiClient
from ..db import get_conn
from ..settings import CONFIG
from .base_loader import insert_marks_final_rows, upsert_sync_state
from .common import json_source_hash

ENDPOINT = "/marks/final"


def ensure_marks_final_partitions(dates: Iterable[date]) -> None:
    months = {d.replace(day=1) for d in dates if d}
    if not months:
        return
    with get_conn() as conn, conn.cursor() as cur:
        for m in sorted(months):
            cur.execute("SELECT raw.ensure_marks_final_partition(%s);", (m,))
        conn.commit()


def fetch_all_finals(client: MojoApiClient) -> List[Dict[str, Any]]:
    """
    Финальные оценки в API без фильтра по датам: забираем всё.
    Ключ массива может быть 'marks' или 'items' — страхуемся.
    """
    data = client.marks_final()
    items = data.get("data", {}).get("marks")
    if items is None:
        items = data.get("data", {}).get("items", [])
    return list(items or [])


from datetime import datetime


def to_raw_rows(items, src_day: date, batch_id: str):
    rows = []
    for it in items:
        # created может быть "2025-10-08 13:59:31+00" или без зоны.
        created_raw = it.get("created")
        created_ts = None
        created_date = None
        if isinstance(created_raw, str) and created_raw.strip():
            try:
                created_ts = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
            except ValueError:
                # на всякий случай: если формат нестандартный — пусть PG сам парсит текст в TIMESTAMPTZ
                created_ts = created_raw
            if isinstance(created_ts, datetime):
                created_date = created_ts.date()

        if not created_date:
            # без даты партиционирования вставлять нельзя — пропускаем запись
            # (можно и fallback=src_day, но лучше не подменять факты)
            continue

        subj = it.get("subject")
        subject = None
        subject_id = None
        if isinstance(subj, int):
            subject_id = subj
        else:
            subject = str(subj) if subj is not None else None

        raw = dict(it)
        rows.append(
            {
                "id": it.get("id"),
                "period": it.get("period"),
                "created_date": created_date,
                "subject": subject,
                "subject_id": subject_id,
                "group_name": it.get("group_name"),
                "id_student": it.get("id_student"),
                "value": it.get("value"),
                "final_criterion": it.get("final_criterion"),
                "assesment": it.get("assesment"),
                "created": (
                    created_ts if isinstance(created_ts, datetime) else created_raw
                ),
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
        )
    return rows


def run_init(d_from: date, d_to: date) -> None:
    client = MojoApiClient()
    batch_id = str(uuid.uuid4())

    items = fetch_all_finals(client)
    # фильтр по created_date
    filt = []
    for it in items:
        cr = it.get("created")
        if not isinstance(cr, str) or not cr.strip():
            continue
        try:
            cd = datetime.fromisoformat(cr.replace("Z", "+00:00")).date()
        except ValueError:
            # если формат странный — попробуем отдать PG как есть, но для фильтра пропустим
            continue
        if d_from <= cd <= d_to:
            filt.append(it)

    rows = to_raw_rows(filt, src_day=date.today(), batch_id=batch_id)
    ensure_marks_final_partitions([r["created_date"] for r in rows])
    inserted = insert_marks_final_rows(rows)
    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=d_from,
        window_to=d_to,
        last_seen_updated_at=datetime.now(),
        params={"mode": "init", "inserted": inserted, "batch_id": batch_id},
        notes="init load marks_final",
    )
    print(f"[marks_final:init] {inserted} rows, window {d_from}..{d_to}")


def run_daily() -> None:
    client = MojoApiClient()
    batch_id = str(uuid.uuid4())

    # финальные оценки редкие → просто забираем все и вставляем, дубликаты отсекутся
    items = fetch_all_finals(client)
    rows = to_raw_rows(items, src_day=date.today(), batch_id=batch_id)
    ensure_marks_final_partitions(
        [r["created_date"] for r in rows if r.get("created_date")]
    )

    inserted = insert_marks_final_rows(rows)
    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=None,
        window_to=None,
        last_seen_updated_at=datetime.now(),
        params={"mode": "daily", "inserted": inserted, "batch_id": batch_id},
        notes="daily load (no server-side date filter)",
    )
    print(f"[marks_final:daily] {inserted} rows")


def run_backfill(days: List[date]) -> None:
    client = MojoApiClient()
    batch_id = str(uuid.uuid4())

    items = fetch_all_finals(client)
    wanted = {d for d in days}
    filt = []
    for it in items:
        cr = it.get("created")
        if not isinstance(cr, str) or not cr.strip():
            continue
        try:
            cd = datetime.fromisoformat(cr.replace("Z", "+00:00")).date()
        except ValueError:
            continue
        if cd in wanted:
            filt.append(it)

    rows = to_raw_rows(filt, src_day=date.today(), batch_id=batch_id)
    ensure_marks_final_partitions([r["created_date"] for r in rows])
    inserted = insert_marks_final_rows(rows)
    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=min(days) if days else None,
        window_to=max(days) if days else None,
        last_seen_updated_at=datetime.now(),
        params={
            "mode": "backfill",
            "inserted": inserted,
            "batch_id": batch_id,
            "days": [d.isoformat() for d in days],
        },
        notes="backfill filtered by created_date",
    )
    print(
        f"[marks_final:backfill] {inserted} rows, days={','.join(sorted(d.isoformat() for d in days))}"
    )


def parse_args():
    p = argparse.ArgumentParser(description="RAW loader for /marks/final")
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
