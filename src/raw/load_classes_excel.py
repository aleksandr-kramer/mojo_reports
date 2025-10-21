# src/raw/load_classes_excel.py
from __future__ import annotations

import argparse
import io
import os
import re
import uuid
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

from ..db import get_conn
from ..settings import CONFIG
from .base_loader import insert_classes_rows, upsert_sync_state
from .common import json_source_hash

ENDPOINT = "excel/classes"

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/gmail.send",
]


# -------- Drive helpers --------
def get_drive():
    sa_path = os.environ.get("GOOGLE_SA_PATH")
    user = os.environ.get("GOOGLE_IMPERSONATE_USER")
    if not sa_path or not user:
        raise SystemExit(
            "GOOGLE_SA_PATH/GOOGLE_IMPERSONATE_USER не заданы в окружении (.env)."
        )
    creds = service_account.Credentials.from_service_account_file(
        sa_path, scopes=SCOPES, subject=user
    )
    return build("drive", "v3", credentials=creds)


def download_xlsx(drive, file_id: str) -> bytes:
    from googleapiclient.http import MediaIoBaseDownload

    buf = io.BytesIO()
    req = drive.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return buf.getvalue()


# -------- header normalization --------
DASHES = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"
DASH_RE = re.compile(f"[{DASHES}]+")


def canon_header(s: str) -> str:
    s = str(s).replace("\u00a0", " ")
    s = DASH_RE.sub("-", s)
    s = s.strip().lower()
    s = re.sub(r"[._/\-]+", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def build_header_map(df: pd.DataFrame) -> Dict[str, str]:
    return {canon_header(c): c for c in df.columns}


# -------- helpers --------
def j(v):
    # безопасно для JSON: NaN -> None
    try:
        if v is None:
            return None
        if isinstance(v, float) and pd.isna(v):
            return None
        if pd.isna(v):
            return None
    except Exception:
        pass
    return v


def norm_cohort(v: Any) -> Optional[str]:
    if (
        v is None
        or (isinstance(v, float) and pd.isna(v))
        or (isinstance(v, str) and v.strip() == "")
    ):
        return None
    try:
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        if isinstance(v, int):
            return str(v)
        s = str(v).strip()
        if re.fullmatch(r"\d+\.0", s):
            return s.split(".")[0]
        return s
    except Exception:
        return str(v).strip()


def to_int(v: Any) -> Optional[int]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if s == "":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


# staff index: фамилия (первый токен) -> список кандидатов
def build_staff_index() -> Dict[str, List[Tuple[str, int, str]]]:
    """
    Возвращает dict: surname_lc -> [(email, staff_id, staff_name), ...]
    """
    sql = "SELECT staff_email, COALESCE(staff_id,0) AS sid, staff_name FROM raw.staff_ref;"
    idx: Dict[str, List[Tuple[str, int, str]]] = {}
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        for email, sid, full in cur.fetchall():
            if not full:
                continue
            tokens = re.split(r"\s+", full.strip())
            if not tokens:
                continue
            surname = tokens[0].lower()
            idx.setdefault(surname, []).append((email, sid if sid != 0 else None, full))
    return idx


def parse_short_staff(s: Any) -> Optional[Tuple[str, str]]:
    """
    'Dolgopolova E.' -> ('dolgopolova', 'E')
    Возвращает None, если пусто или не распарсилось.
    """
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    t = str(s).strip()
    if not t:
        return None
    # допускаем варианты 'Surname I.' или 'Surname I' (без точки)
    m = re.match(r"^([A-Za-z\-']+)\s+([A-Za-z])[\.]?$", t)
    if not m:
        return None
    return (m.group(1).lower(), m.group(2).upper())


def choose_homeroom(
    idx: Dict[str, List[Tuple[str, int, str]]], short: str
) -> Tuple[Optional[str], Optional[int], str]:
    """
    Пытается найти руководителя по короткому имени.
    Возвращает (email, staff_id, status)
      status: 'matched' / 'not_found' / 'ambiguous'
    """
    parsed = parse_short_staff(short)
    if not parsed:
        return (None, None, "not_found")
    surname_lc, initial = parsed
    cands = idx.get(surname_lc, [])
    if not cands:
        return (None, None, "not_found")

    # фильтр по инициалу имени (берём первую букву второго токена полного имени)
    def first_initial(full: str) -> Optional[str]:
        toks = re.split(r"\s+", full.strip())
        if len(toks) >= 2:
            return toks[1][0].upper()
        return None

    good = [
        (e, sid, full) for (e, sid, full) in cands if first_initial(full) == initial
    ]
    if len(good) == 1:
        e, sid, _ = good[0]
        return (e, sid, "matched")
    if len(good) == 0:
        return (None, None, "not_found")
    # несколько с одинаковой фамилией и инициалом — неоднозначно
    return (None, None, "ambiguous")


# -------- core normalize --------
def normalize_rows(
    df: pd.DataFrame, src_day: date, batch_id: str, overrides: Dict[str, str]
):
    hdr = build_header_map(df)
    title_col = hdr.get(canon_header("Title"))
    cohort_col = hdr.get(canon_header("Cohort"))
    staff_col = hdr.get(canon_header("Staff member"))
    num_col = hdr.get(canon_header("Number of students"))

    missing = [
        n
        for n, c in {
            "Title": title_col,
            "Cohort": cohort_col,
            "Staff member": staff_col,
            "Number of students": num_col,
        }.items()
        if c is None
    ]
    if missing:
        raise ValueError(
            f"В Excel нет ожидаемых колонок: {missing}. Фактические: {list(df.columns)}"
        )

    staff_idx = build_staff_index()

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        title = r.get(title_col)
        if (
            title is None
            or (isinstance(title, float) and pd.isna(title))
            or str(title).strip() == ""
        ):
            continue
        title_str = str(title).strip()

        cohort = norm_cohort(r.get(cohort_col))
        short = None if pd.isna(r.get(staff_col)) else str(r.get(staff_col)).strip()
        num = to_int(r.get(num_col))

        # override по названию класса (если указан в конфиге)
        hom_email = None
        hom_id = None
        status = "not_found"
        method = None

        if overrides and title_str in overrides and overrides[title_str]:
            hom_email = overrides[title_str].strip().lower()
            # staff_id подтянем из staff_ref при наличии email
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT staff_id FROM raw.staff_ref WHERE staff_email=%s;",
                    (hom_email,),
                )
                row = cur.fetchone()
                hom_id = row[0] if row else None
            status = "matched"
            method = "override"
        else:
            if short:
                hom_email, hom_id, status = choose_homeroom(staff_idx, short)
                method = "surname+initial"

        raw_obj = {
            "Title": j(title_str),
            "Cohort": j(cohort),
            "Staff member": j(short),
            "Number of students": j(num),
            "homeroom_email": j(hom_email),
            "homeroom_staff_id": j(hom_id),
            "match_status": status,
            "match_method": method,
        }

        rows.append(
            {
                "title": title_str,
                "cohort": cohort,
                "homeroom_short": short,
                "students_count": num,
                "homeroom_email": hom_email,
                "homeroom_staff_id": hom_id,
                "match_status": status,
                "match_method": method,
                "first_seen_src_day": src_day,
                "last_seen_src_day": src_day,
                "src_day": src_day,
                "source_system": "drive",
                "endpoint": ENDPOINT,
                "raw_json": raw_obj,
                "ingested_at": datetime.now(),
                "source_hash": json_source_hash(raw_obj),
                "batch_id": batch_id,
            }
        )

    return rows


def run():
    drive = get_drive()
    batch_id = str(uuid.uuid4())
    today = date.today()

    file_id = CONFIG["excel"]["drive"]["classes_id"]
    blob = download_xlsx(drive, file_id)
    df = pd.read_excel(io.BytesIO(blob), engine="openpyxl")

    overrides = (
        CONFIG["excel"].get("classes_overrides", {}) if "excel" in CONFIG else {}
    )

    rows = normalize_rows(df, src_day=today, batch_id=batch_id, overrides=overrides)
    inserted = insert_classes_rows(rows)

    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=None,
        window_to=None,
        last_seen_updated_at=datetime.now(),
        params={
            "mode": "daily",
            "inserted": inserted,
            "batch_id": batch_id,
            "count_rows": len(rows),
        },
        notes="excel classes load",
    )

    print(f"[excel:classes] upserted {inserted} rows (source rows: {len(rows)})")


def main():
    parser = argparse.ArgumentParser(description="RAW loader: Excel classes")
    parser.add_argument("--run", action="store_true")
    _ = parser.parse_args()
    run()


if __name__ == "__main__":
    main()
