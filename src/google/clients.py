import json
import os
from typing import Iterable, Optional, Tuple

from googleapiclient.discovery import build

from google.auth.transport.requests import Request
from google.oauth2 import service_account

# В проекте уже есть загрузка config в src/settings.py
# Используем её, чтобы не дублировать чтение конфигурации.
try:
    from ..settings import CONFIG
except Exception:
    CONFIG = {}

# Обязательные скоупы (минимум под отчёты)
SCOPE_DRIVE = "https://www.googleapis.com/auth/drive"
SCOPE_SLIDES = "https://www.googleapis.com/auth/presentations"
SCOPE_GMAIL_SEND = "https://www.googleapis.com/auth/gmail.send"

DEFAULT_SCOPES = [SCOPE_DRIVE, SCOPE_SLIDES, SCOPE_GMAIL_SEND]


def _strip_quotes(value: Optional[str]) -> Optional[str]:
    """Удаляет обрамляющие двойные/одинарные кавычки у переменной окружения, если они есть."""
    if value is None:
        return None
    v = value.strip()
    if (v.startswith('"') and v.endswith('"')) or (
        v.startswith("'") and v.endswith("'")
    ):
        return v[1:-1]
    return v


def _load_sa_path() -> str:
    """
    Путь к ключу сервисного аккаунта:
    1) Берём из .env GOOGLE_SA_PATH
    2) Если нет, пытаемся прочитать из CONFIG['google']['sa_path'] (опционально, если когда-либо добавится)
    """
    env_path = _strip_quotes(os.getenv("GOOGLE_SA_PATH"))
    if env_path:
        return env_path

    # Фолбэк на конфиг (не обязателен в текущей версии)
    google_cfg = CONFIG.get("google", {}) if isinstance(CONFIG, dict) else {}
    cfg_path = google_cfg.get("sa_path")
    if cfg_path:
        return cfg_path

    raise RuntimeError("GOOGLE_SA_PATH is not set. Specify it in .env")


def _load_impersonate_user() -> str:
    """
    Импёрсонация пользователя:
    1) Берём из .env GOOGLE_IMPERSONATE_USER
    2) Если нет, пытаемся прочитать из reports.email.sender (как разумный дефолт)
    """
    env_user = _strip_quotes(os.getenv("GOOGLE_IMPERSONATE_USER"))
    if env_user:
        return env_user

    reports_cfg = CONFIG.get("reports", {}) if isinstance(CONFIG, dict) else {}
    email_cfg = reports_cfg.get("email", {})
    sender = email_cfg.get("sender")
    if sender:
        return sender

    raise RuntimeError("GOOGLE_IMPERSONATE_USER is not set. Specify it in .env")


def get_delegated_credentials(scopes: Iterable[str] = DEFAULT_SCOPES):
    """
    Создаёт делегированные (impersonated) учетные данные на основе Service Account.
    Требуется включенная Domain-wide delegation у SA и права impersonation на пользователя.
    """
    sa_path = _load_sa_path()
    user = _load_impersonate_user()

    credentials = service_account.Credentials.from_service_account_file(
        sa_path,
        scopes=list(scopes),
    )
    delegated = credentials.with_subject(user)

    # Обновляем токен при необходимости
    if not delegated.valid:
        request = Request()
        delegated.refresh(request)

    return delegated


def build_services(
    scopes: Iterable[str] = DEFAULT_SCOPES,
    drive_version: str = "v3",
    slides_version: str = "v1",
    gmail_version: str = "v1",
) -> Tuple:
    """
    Возвращает кортеж (drive, slides, gmail) — клиенты Google API.
    Можно вызывать и частично (например, только drive), передав нужные скоупы и игнорируя остальное.
    """
    creds = get_delegated_credentials(scopes=scopes)

    # Строим сервисы. Если какой-то не нужен — можно не использовать его в вызывающем коде.
    drive = build("drive", drive_version, credentials=creds, cache_discovery=False)
    slides = build("slides", slides_version, credentials=creds, cache_discovery=False)
    gmail = build("gmail", gmail_version, credentials=creds, cache_discovery=False)

    return drive, slides, gmail
