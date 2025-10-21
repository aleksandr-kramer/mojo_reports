# src/raw/load_parents_excel.py
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
from .base_loader import (
    insert_parent_links_rows,
    insert_parents_rows,
    upsert_sync_state,
)
from .common import json_source_hash

ENDPOINT_PARENTS = "excel/parents"
ENDPOINT_LINKS = "excel/parents_links"

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


# -------- Excel parsing --------
DASHES = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"
DASH_RE = re.compile(f"[{DASHES}]+")


def canon_header(s: str) -> str:
    s = str(s).replace("\u00a0", " ")
    s = DASH_RE.sub("-", s)
    s = s.strip().lower()
    s = re.sub(r"[._/\-]+", "", s)  # e-mail -> email
    s = re.sub(r"\s+", " ", s)
    return s


def build_header_map(df: pd.DataFrame) -> Dict[str, str]:
    return {canon_header(c): c for c in df.columns}


def get_sid(v) -> Optional[int]:
    if pd.isna(v):
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(str(v).strip())
        except Exception:
            return None


def norm_email(v: Any) -> Optional[str]:
    if pd.isna(v):
        return None
    s = str(v).strip().lower()
    return s or None


def norm_name(s: Any) -> Optional[str]:
    if pd.isna(s):
        return None
    s = re.sub(r"\s+", " ", str(s).strip())
    return s or None


def norm_grade(v: Any) -> Optional[str]:
    if pd.isna(v):
        return None
    try:
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        if isinstance(v, int):
            return str(v)
        s = str(v).strip()
        if re.fullmatch(r"\d+\.0", s):
            return s.split(".")[0]
        return s or None
    except Exception:
        return str(v).strip()


# сопоставление "Фамилия Имя + Cohort" -> student_id из raw.students_ref
def build_student_index() -> Dict[Tuple[str, str], int]:
    sql = """
      SELECT student_id,
             lower(trim(regexp_replace(last_name || ' ' || first_name, '\\s+', ' ', 'g'))) AS full_name_lc,
             cohort
      FROM raw.students_ref;
    """
    out: Dict[Tuple[str, str], int] = {}
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        for sid, full_name_lc, cohort in cur.fetchall():
            key = (full_name_lc or "", (cohort or "").strip())
            out[key] = sid
    return out


def normalize_rows(df: pd.DataFrame, src_day: date, batch_id: str):
    hdr = build_header_map(df)

    id_col = hdr.get(canon_header("Id"))
    parent_col = hdr.get(canon_header("Parent"))
    student_col = hdr.get(canon_header("Student"))
    grade_col = hdr.get(canon_header("Grade"))
    email_col = hdr.get(canon_header("E-mail"))

    missing = [
        n
        for n, c in {
            "Id": id_col,
            "Parent": parent_col,
            "Student": student_col,
            "Grade": grade_col,
            "E-mail": email_col,
        }.items()
        if c is None
    ]
    if missing:
        raise ValueError(
            f"В Excel нет ожидаемых колонок: {missing}. Фактические: {list(df.columns)}"
        )

    # Индекс учащихся для сопоставления (full_name_lc + cohort -> student_id)
    stud_index = build_student_index()

    parents_seen: Dict[str, Dict[str, Any]] = {}  # email -> parent_row
    links_map: Dict[Tuple[str, str, str], Dict[str, Any]] = (
        {}
    )  # (email, student_name, grade_norm) -> link_row

    for _, row in df.iterrows():
        sid_val = get_sid(row.get(id_col))
        parent_name = norm_name(row.get(parent_col))
        student_nm = norm_name(row.get(student_col))
        grade_txt = norm_grade(row.get(grade_col))
        email_lc = norm_email(row.get(email_col))

        # правила пропуска
        if sid_val is None and (parent_name is None or parent_name == ""):
            continue  # A и B пустые => строка не нужна
        if not email_lc:
            continue  # пустой e-mail => строка не нужна

        # Родитель (апсерт по email)
        if email_lc not in parents_seen:
            raw_p = {
                "Id": row.get(id_col),
                "Parent": row.get(parent_col),
                "E-mail": row.get(email_col),
            }
            parents_seen[email_lc] = {
                "parent_email": email_lc,
                "parent_id": sid_val,
                "parent_name": parent_name,
                "first_seen_src_day": src_day,
                "last_seen_src_day": src_day,
                "src_day": src_day,
                "source_system": "drive",
                "endpoint": ENDPOINT_PARENTS,
                "raw_json": raw_p,
                "ingested_at": datetime.now(),
                "source_hash": json_source_hash(raw_p),
                "batch_id": batch_id,
            }
        else:
            # аккуратно «дополняем» ранее увиденную запись
            if parents_seen[email_lc]["parent_id"] is None and sid_val is not None:
                parents_seen[email_lc]["parent_id"] = sid_val
            if not parents_seen[email_lc]["parent_name"] and parent_name:
                parents_seen[email_lc]["parent_name"] = parent_name
            parents_seen[email_lc]["last_seen_src_day"] = src_day
            parents_seen[email_lc]["src_day"] = src_day

        # Связь родитель↔ученик
        if student_nm:
            raw_l = {
                "Parent": row.get(parent_col),
                "Student": row.get(student_col),
                "Grade": row.get(grade_col),
                "E-mail": row.get(email_col),
                "Id": row.get(id_col),
            }
            # нормализованный grade для ключа/PK (не NULL)
            grade_norm = grade_txt or ""
            key = (email_lc, student_nm, grade_norm)

            # подберём student_id через индекс (сопоставляем lc(Фамилия Имя) + cohort)
            stud_id = None
            full_name_lc = (student_nm or "").lower()
            # cohort в индексе хранится как текст (без .0), мы туда кладём grade_norm
            stud_id = stud_index.get((full_name_lc, grade_norm))

            new_link = {
                "parent_email": email_lc,
                "student_name": student_nm,
                "grade": grade_norm,  # важно: не NULL
                "student_id": stud_id,
                "parent_id": sid_val,  # <<< добавили сюда id из столбца A (если был)
                "first_seen_src_day": src_day,
                "last_seen_src_day": src_day,
                "src_day": src_day,
                "source_system": "drive",
                "endpoint": ENDPOINT_LINKS,
                "raw_json": raw_l,
                "ingested_at": datetime.now(),
                "source_hash": json_source_hash(raw_l),
                "batch_id": batch_id,
            }

            # дедупликация внутри батча: держим одну запись на ключ
            if key not in links_map:
                links_map[key] = new_link
            else:
                # Улучшаем запись: если раньше student_id не было, а сейчас есть — проставим
                if (
                    links_map[key]["student_id"] is None
                    and new_link["student_id"] is not None
                ):
                    links_map[key]["student_id"] = new_link["student_id"]
                # Обновим служебные метки «последний раз видели»
                links_map[key]["last_seen_src_day"] = src_day
                links_map[key]["src_day"] = src_day
                # можно также обновить raw_json на «последний»
                links_map[key]["raw_json"] = new_link["raw_json"]
                links_map[key]["source_hash"] = new_link["source_hash"]
                links_map[key]["ingested_at"] = new_link["ingested_at"]

    parents_rows = list(parents_seen.values())
    links_rows = list(links_map.values())
    return parents_rows, links_rows


def run():
    drive = get_drive()
    batch_id = str(uuid.uuid4())
    today = date.today()

    file_id = CONFIG["excel"]["drive"]["parents_id"]
    blob = download_xlsx(drive, file_id)
    df = pd.read_excel(io.BytesIO(blob), engine="openpyxl")

    parents_rows, links_rows = normalize_rows(df, src_day=today, batch_id=batch_id)

    ins_p = insert_parents_rows(parents_rows)
    ins_l = insert_parent_links_rows(links_rows)

    upsert_sync_state(
        endpoint=ENDPOINT_PARENTS,
        window_from=None,
        window_to=None,
        last_seen_updated_at=datetime.now(),
        params={
            "mode": "daily",
            "inserted_parents": ins_p,
            "inserted_links": ins_l,
            "batch_id": batch_id,
            "parents_rows": len(parents_rows),
            "links_rows": len(links_rows),
        },
        notes="excel parents load",
    )
    print(
        f"[excel:parents] upserted parents={ins_p}, links={ins_l} "
        f"(source parents={len(parents_rows)}, source links={len(links_rows)})"
    )


def main():
    parser = argparse.ArgumentParser(description="RAW loader: Excel parents")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    run()


if __name__ == "__main__":
    main()
