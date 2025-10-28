# src/core/core_common.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from ..db import get_conn
from ..settings import CONFIG

# ──────────────────────────────────────────────────────────────────────────────
# Общие модели/типы


@dataclass
class SyncState:
    endpoint: str
    window_from: Optional[date]
    window_to: Optional[date]
    last_successful_sync_at: Optional[datetime]
    params: Dict[str, Any]


# ──────────────────────────────────────────────────────────────────────────────
# Безопасный JSON (без NaN/Infinity) — годится для JSONB в Postgres


def json_dumps_safe(obj: Any) -> str:
    """
    json.dumps с запретом NaN/Infinity (Postgres JSON этого не понимает).
    """
    return json.dumps(obj, ensure_ascii=False, allow_nan=False)


def json_param(obj: Any) -> psycopg2.extras.Json:
    """
    Обёртка для psycopg2, чтобы класть dict/list в JSONB-поля.
    """
    return psycopg2.extras.Json(obj, dumps=json_dumps_safe)


# ──────────────────────────────────────────────────────────────────────────────
# Нормализации/утилиты


def as_bool_from_int(v: Optional[int]) -> Optional[bool]:
    if v is None:
        return None
    return bool(int(v))


def to_str_cohort(v: Any) -> Optional[str]:
    """
    '12.0' -> '12', 12 -> '12', None/'' -> None
    """
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if s.endswith(".0"):
        return s[:-2]
    return s


def programme_to_code(name: Optional[str]) -> Optional[str]:
    """
    Маппинг «человекочитаемого» названия программы в код из CORE.
    Возвращает None, если не распознано.
    """
    if not name:
        return None
    n = name.strip().lower()
    if n in {"pearson", "pearson programme", "pearson edexcel"}:
        return "PEARSON"
    if n in {"ipc", "international primary curriculum"}:
        return "IPC"
    if n in {"ib", "ib dp", "ib diploma", "ib diploma programme"}:
        return "IB"
    if n in {
        "state standard",
        "montenegrin state",
        "state",
        "national curriculum",
        "national programme",
        "national program",
    }:
        return "STATE"
    return None


def today_utc_date() -> date:
    # Даты в БД в UTC; для дневных окон нам достаточно текущей utc-даты
    return datetime.utcnow().date()


def daterange(d_from: date, d_to: date) -> Iterator[date]:
    """
    Включительно: d_from..d_to
    """
    d = d_from
    while d <= d_to:
        yield d
        d = d + timedelta(days=1)


def chunk_window(
    d_from: date, d_to: date, chunk_days: int
) -> Iterator[Tuple[date, date]]:
    """
    Делит окно дат на куски фиксированного размера (включительно).
    """
    if chunk_days <= 0:
        yield (d_from, d_to)
        return
    cur = d_from
    while cur <= d_to:
        end = min(cur + timedelta(days=chunk_days - 1), d_to)
        yield (cur, end)
        cur = end + timedelta(days=1)


# ──────────────────────────────────────────────────────────────────────────────
# sync_state для CORE


def read_sync_state(endpoint: str) -> Optional[SyncState]:
    """
    Читает состояние по endpoint из core.sync_state.
    """
    sql = """
      SELECT endpoint, window_from, window_to, last_successful_sync_at, COALESCE(params, '{}'::jsonb)
      FROM core.sync_state
      WHERE endpoint = %s
      LIMIT 1;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (endpoint,))
        row = cur.fetchone()
        if not row:
            return None
        return SyncState(
            endpoint=row[0],
            window_from=row[1],
            window_to=row[2],
            last_successful_sync_at=row[3],
            params=row[4] or {},
        )


def upsert_sync_state(
    endpoint: str,
    window_from: Optional[date],
    window_to: Optional[date],
    last_seen_updated_at: Optional[datetime],
    params: Optional[Dict[str, Any]] = None,
    notes: Optional[str] = None,
) -> None:
    """
    Обновляет/вставляет запись о прохождении загрузчика CORE.
    ВАЖНО: если last_seen_updated_at не передан (None), last_successful_sync_at фиксируется как now().
    """
    sql = """
      INSERT INTO core.sync_state(endpoint, window_from, window_to, last_successful_sync_at, params, notes)
      VALUES (%s, %s, %s, COALESCE(%s, now()), %s, %s)
      ON CONFLICT (endpoint) DO UPDATE
        SET window_from = EXCLUDED.window_from,
            window_to   = EXCLUDED.window_to,
            last_successful_sync_at = COALESCE(EXCLUDED.last_successful_sync_at, now()),
            params      = EXCLUDED.params,
            notes       = EXCLUDED.notes;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            sql,
            (
                endpoint,
                window_from,
                window_to,
                last_seen_updated_at,  # может быть None; тогда в INSERT/UPDATE подставится now()
                json_param(params or {}),  # как и раньше
                notes,
            ),
        )
        conn.commit()


def compute_daily_window(days_back: int = 14) -> Tuple[date, date]:
    """
    Консервативное дневное окно: последние N дней включительно.
    Идемпотентно для FACT (upsert по натуральному ключу).
    """
    today = today_utc_date()
    d_from = today - timedelta(days=days_back - 1)
    return (d_from, today)


def compute_init_window(d_from: date, d_to: date) -> Tuple[date, date]:
    """
    Окно для первичной загрузки.
    """
    return (min(d_from, d_to), max(d_from, d_to))


# ──────────────────────────────────────────────────────────────────────────────
# Небольшие SQL-утилиты


def exec_sql(sql: str, params: Optional[Tuple[Any, ...]] = None) -> int:
    """
    Выполняет одиночный SQL, возвращает cur.rowcount (если применимо).
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        rc = cur.rowcount
        conn.commit()
    return rc


def fetchall(
    sql: str, params: Optional[Tuple[Any, ...]] = None
) -> List[Tuple[Any, ...]]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def fetchone(
    sql: str, params: Optional[Tuple[Any, ...]] = None
) -> Optional[Tuple[Any, ...]]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()


# ──────────────────────────────────────────────────────────────────────────────
# Логирование (просто консоль)


def log(msg: str) -> None:
    print(msg, flush=True)


# ──────────────────────────────────────────────────────────────────────────────
# Примитивные валидаторы окон (на случай ручного запуска)


def validate_window_or_throw(d_from: date, d_to: date) -> None:
    if d_from > d_to:
        raise ValueError(f"Некорректное окно дат: {d_from}..{d_to}")


CORE_ORCHESTRATOR_ENDPOINT = "core:orchestrator"


def get_core_checkpoint() -> date | None:
    sql = """
      SELECT window_to::date
      FROM core.sync_state
      WHERE endpoint = %s
      ORDER BY last_successful_sync_at DESC
      LIMIT 1;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (CORE_ORCHESTRATOR_ENDPOINT,))
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def set_core_checkpoint(window_to: date) -> None:
    """
    Фиксируем чекпойнт CORE. В таблице core.sync_state PK = (endpoint),
    поэтому конфликт ловим по endpoint и там же обновляем окно.
    Окно расширяем монотонно:
      window_from = least(старое, новое)
      window_to   = greatest(старое, новое)
    """
    sql = """
      INSERT INTO core.sync_state(endpoint, window_from, window_to, last_successful_sync_at)
      VALUES (%s, %s, %s, now())
      ON CONFLICT (endpoint) DO UPDATE
        SET window_from = LEAST(COALESCE(core.sync_state.window_from, EXCLUDED.window_from), EXCLUDED.window_from),
            window_to   = GREATEST(COALESCE(core.sync_state.window_to, EXCLUDED.window_to), EXCLUDED.window_to),
            last_successful_sync_at = EXCLUDED.last_successful_sync_at;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (CORE_ORCHESTRATOR_ENDPOINT, window_to, window_to))
        conn.commit()
