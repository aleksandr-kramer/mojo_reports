# src/raw/load_staff_excel.py
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

from ..settings import CONFIG
from .base_loader import (
    insert_staff_positions_rows,
    insert_staff_rows,
    upsert_sync_state,
)
from .common import json_source_hash

ENDPOINT_STAFF = "excel/staff"
ENDPOINT_POS = "excel/staff_positions"

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/gmail.send",
]


# ---------- Drive helpers ----------
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


# ---------- header normalization ----------
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


# ---------- little helpers ----------
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


def norm_key(s: Any) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = re.sub(r"\s+", " ", str(s).strip().lower())
    return s


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


# ---------- core normalize ----------
def normalize_rows(df: pd.DataFrame, src_day: date, batch_id: str):
    hdr = build_header_map(df)

    id_col = hdr.get(canon_header("Id"))
    name_col = hdr.get(canon_header("Staff"))
    gender_col = hdr.get(canon_header("Gender"))
    email_col = hdr.get(canon_header("E-mail"))
    dept_col = hdr.get(canon_header("Department"))
    pos_col = hdr.get(canon_header("Position"))

    missing = [
        n
        for n, c in {"Id": id_col, "Staff": name_col, "E-mail": email_col}.items()
        if c is None
    ]
    if missing:
        raise ValueError(
            f"В Excel нет ожидаемых колонок: {missing}. Фактические: {list(df.columns)}"
        )

    staff_seen: Dict[str, Dict[str, Any]] = {}  # email -> staff row
    pos_map: Dict[Tuple[str, str, str], Dict[str, Any]] = (
        {}
    )  # (email, dept_key, pos_key) -> pos row

    for _, row in df.iterrows():
        staff_id = get_sid(row.get(id_col))
        staff_nm = norm_name(row.get(name_col))
        email = norm_email(row.get(email_col))
        if not email:
            continue  # без email не сможем апсертить — пропускаем

        # --- NEW: gender из столбца D ---
        gender_txt = None
        if gender_col is not None:
            gval = row.get(gender_col)
            if not pd.isna(gval):
                gender_txt = str(gval).strip()

        dept_raw = None if dept_col is None else row.get(dept_col)
        pos_raw = None if pos_col is None else row.get(pos_col)

        dept_txt = None if pd.isna(dept_raw) else str(dept_raw).strip()
        pos_txt = None if pd.isna(pos_raw) else str(pos_raw).strip()

        def j(v):
            # безопасное значение для JSON: NaN -> None
            if v is None:
                return None
            try:
                import pandas as pd

                if pd.isna(v):
                    return None
            except Exception:
                pass
            return v

        dept_key = norm_key(dept_txt)  # '' если пусто
        pos_key = norm_key(pos_txt)  # '' если пусто

        # --- staff_ref (upsert by email) ---
        if email not in staff_seen:
            raw_s = {
                "Id": j(staff_id),
                "Staff": j(staff_nm),
                "E-mail": j(email),
                "Gender": j(gender_txt),  # <<< добавили
                "Department": j(dept_txt),
                "Position": j(pos_txt),
            }

            staff_seen[email] = {
                "staff_email": email,
                "gender": gender_txt,
                "staff_id": staff_id,
                "staff_name": staff_nm,
                "first_seen_src_day": src_day,
                "last_seen_src_day": src_day,
                "src_day": src_day,
                "source_system": "drive",
                "endpoint": ENDPOINT_STAFF,
                "raw_json": raw_s,
                "ingested_at": datetime.now(),
                "source_hash": json_source_hash(raw_s),
                "batch_id": batch_id,
            }
        else:
            if staff_seen[email]["staff_id"] is None and staff_id is not None:
                staff_seen[email]["staff_id"] = staff_id
            if not staff_seen[email]["staff_name"] and staff_nm:
                staff_seen[email]["staff_name"] = staff_nm
            if not staff_seen[email].get("gender") and gender_txt:  # <— добавили
                staff_seen[email]["gender"] = gender_txt
            staff_seen[email]["last_seen_src_day"] = src_day
            staff_seen[email]["src_day"] = src_day

        # --- staff_positions (even if dept/pos empty) ---
        key = (email, dept_key, pos_key)
        raw_p = {
            "E-mail": j(email),
            "Department": j(dept_txt),
            "Position": j(pos_txt),
            "Id": j(staff_id),
            "Staff": j(staff_nm),
        }

        row_pos = {
            "staff_email": email,
            "department": dept_txt,
            "position": pos_txt,
            "department_key": dept_key,
            "position_key": pos_key,
            "first_seen_src_day": src_day,
            "last_seen_src_day": src_day,
            "src_day": src_day,
            "source_system": "drive",
            "endpoint": ENDPOINT_POS,
            "raw_json": raw_p,
            "ingested_at": datetime.now(),
            "source_hash": json_source_hash(raw_p),
            "batch_id": batch_id,
        }
        # дедупликация в батче
        if key not in pos_map:
            pos_map[key] = row_pos
        else:
            # если раньше department/position были пустыми, а сейчас появились — подменим на заполненные
            if not pos_map[key]["department"] and row_pos["department"]:
                pos_map[key]["department"] = row_pos["department"]
            if not pos_map[key]["position"] and row_pos["position"]:
                pos_map[key]["position"] = row_pos["position"]
            pos_map[key]["last_seen_src_day"] = src_day
            pos_map[key]["src_day"] = src_day
            pos_map[key]["raw_json"] = row_pos["raw_json"]
            pos_map[key]["source_hash"] = row_pos["source_hash"]
            pos_map[key]["ingested_at"] = row_pos["ingested_at"]

    staff_rows = list(staff_seen.values())
    pos_rows = list(pos_map.values())
    return staff_rows, pos_rows


def run():
    drive = get_drive()
    batch_id = str(uuid.uuid4())
    today = date.today()

    file_id = CONFIG["excel"]["drive"]["staff_id"]
    blob = download_xlsx(drive, file_id)
    df = pd.read_excel(io.BytesIO(blob), engine="openpyxl")

    staff_rows, pos_rows = normalize_rows(df, src_day=today, batch_id=batch_id)

    ins_staff = insert_staff_rows(staff_rows)
    ins_pos = insert_staff_positions_rows(pos_rows)

    upsert_sync_state(
        endpoint=ENDPOINT_STAFF,
        window_from=None,
        window_to=None,
        last_seen_updated_at=datetime.now(),
        params={
            "mode": "daily",
            "inserted_staff": ins_staff,
            "inserted_positions": ins_pos,
            "batch_id": batch_id,
            "staff_rows": len(staff_rows),
            "pos_rows": len(pos_rows),
        },
        notes="excel staff load",
    )
    print(
        f"[excel:staff] upserted staff={ins_staff}, positions={ins_pos} "
        f"(source staff={len(staff_rows)}, source positions={len(pos_rows)})"
    )


def main():
    parser = argparse.ArgumentParser(description="RAW loader: Excel staff")
    parser.add_argument("--run", action="store_true")
    _ = parser.parse_args()
    run()


if __name__ == "__main__":
    main()
