# src/raw/common.py
from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any, Dict, Iterable, List, Set

from ..db import get_conn


def json_source_hash(obj: Dict[str, Any]) -> str:
    """
    Стабильная контрольная сумма по JSON: сортируем ключи, без пробелов.
    """
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def month_starts(dates: Iterable[date]) -> Set[date]:
    out: Set[date] = set()
    for d in dates:
        out.add(d.replace(day=1))
    return out


def ensure_attendance_partitions(dates: Iterable[date]) -> None:
    """
    Гарантирует наличие месячных партиций для всех дат из набора.
    """
    months = month_starts(dates)
    if not months:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            for m in sorted(months):
                cur.execute("SELECT raw.ensure_attendance_partition(%s);", (m,))
        conn.commit()
