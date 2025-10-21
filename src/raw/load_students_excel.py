# src/raw/load_students_excel.py
from __future__ import annotations

import argparse
import io
import os
import re
import uuid
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

from ..settings import CONFIG
from .base_loader import insert_students_rows, upsert_sync_state
from .common import json_source_hash

ENDPOINT = "excel/students"

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/gmail.send",
]


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
    # Скачиваем бинарник XLSX (get_media)
    from googleapiclient.http import MediaIoBaseDownload

    buf = io.BytesIO()
    req = drive.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return buf.getvalue()


def parse_date(val) -> Optional[date]:
    if pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


# --- нормализация заголовков -------------------------------------------------
DASHES = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"  # разные «дефисы»
DASH_RE = re.compile(f"[{DASHES}]+")


def canon_header(s: str) -> str:
    """
    Приводим заголовок к канону:
    - заменяем неразрывные пробелы на обычные
    - приводим к lower()
    - убираем точки/подчёркивания/слэши/дефисы
    - схлопываем пробелы
    Примеры: 'E-mail' → 'email', 'First name' → 'firstname'
    """
    s = str(s).replace("\u00a0", " ")  # nbsp -> space
    s = DASH_RE.sub("-", s)  # любые тире -> обычный '-'
    s = s.strip().lower()
    s = re.sub(r"[._/\-]+", "", s)  # e-mail, e_mail, e/mail -> email
    s = re.sub(r"\s+", " ", s)
    return s


def build_header_map(df: pd.DataFrame) -> Dict[str, str]:
    """
    Возвращает словарь: каноничное имя -> реальное имя колонки из Excel.
    """
    m: Dict[str, str] = {}
    for c in df.columns:
        m[canon_header(c)] = c
    return m


def pick(hdr_map: Dict[str, str], *candidates: str) -> Optional[str]:
    """
    Находит первую подходящую колонку по списку канонических имен.
    """
    for c in candidates:
        cc = canon_header(c)
        if cc in hdr_map:
            return hdr_map[cc]
    return None


def normalize_rows(
    df: pd.DataFrame, src_day: date, batch_id: str
) -> List[Dict[str, Any]]:
    # Построим карту заголовков
    hdr = build_header_map(df)

    id_col = pick(hdr, "id", "student id", "student_id")
    lname_col = pick(hdr, "last name", "lastname", "surname", "фамилия")
    fname_col = pick(hdr, "first name", "firstname", "имя")
    gender_col = pick(hdr, "gender", "пол")
    dob_col = pick(
        hdr, "date of birth", "dateofbirth", "dob", "дата рождения", "датарождения"
    )
    email_col = pick(hdr, "e-mail", "email", "почта", "email address")
    cohort_col = pick(hdr, "cohort", "параллель", "год")
    class_col = pick(hdr, "class", "class name", "classname", "класс", "title")
    program_col = pick(hdr, "program", "программа")

    required_missing = [
        name
        for name, col in {
            "Id": id_col,
            "Last name": lname_col,
            "First name": fname_col,
            "Gender": gender_col,
            "Date of birth": dob_col,
            "E-mail": email_col,
            "Cohort": cohort_col,
            "Class": class_col,
            # Program не обязательно; если нет — будет NULL
        }.items()
        if col is None
    ]

    if required_missing:
        raise ValueError(
            f"Отсутствуют ожидаемые колонки в Excel: {required_missing}\n"
            f"Фактические: {list(df.columns)}"
        )

    def get_str(row, col):
        if col is None:
            return None
        v = row.get(col)
        if pd.isna(v):
            return None
        s = str(v).strip()
        return s if s else None

    def get_cohort(row, col):
        v = row.get(col)
        if pd.isna(v):
            return None
        # 6.0 -> "6"
        try:
            if isinstance(v, float) and v.is_integer():
                return str(int(v))
            if isinstance(v, (int,)):
                return str(v)
            s = str(v).strip()
            # на всякий случай "6.0" строкой
            if re.fullmatch(r"\d+\.0", s):
                return s.split(".")[0]
            return s
        except Exception:
            return str(v).strip()

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

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        sid = get_sid(r.get(id_col))
        if sid is None:
            continue

        raw = {
            k: (None if (isinstance(v, float) and pd.isna(v)) else v)
            for k, v in r.to_dict().items()
        }

        row = {
            "student_id": sid,
            "first_name": get_str(r, fname_col),
            "last_name": get_str(r, lname_col),
            "gender": get_str(r, gender_col),
            "dob": parse_date(r.get(dob_col)) if dob_col else None,
            "email": get_str(r, email_col),
            "cohort": get_cohort(r, cohort_col),
            "class_name": get_str(r, class_col),
            "program": get_str(r, program_col),
            # Excel-колонки L–O (родители) игнорируем
            "parents_raw": None,
            "first_seen_src_day": src_day,
            "last_seen_src_day": src_day,
            "src_day": src_day,
            "source_system": "drive",
            "endpoint": ENDPOINT,
            "raw_json": raw,
            "ingested_at": datetime.now(),
            "source_hash": json_source_hash(raw),
            "batch_id": batch_id,
        }
        rows.append(row)

    return rows


def run():
    drive = get_drive()
    batch_id = str(uuid.uuid4())
    today = date.today()

    file_id = CONFIG["excel"]["drive"]["students_id"]
    blob = download_xlsx(drive, file_id)
    df = pd.read_excel(io.BytesIO(blob), engine="openpyxl")

    rows = normalize_rows(df, src_day=today, batch_id=batch_id)
    inserted = insert_students_rows(rows)

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
        notes="excel students load",
    )
    print(f"[excel:students] upserted {inserted} rows (source rows: {len(rows)})")


def main():
    parser = argparse.ArgumentParser(description="RAW loader: Excel students")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    run()


if __name__ == "__main__":
    main()
