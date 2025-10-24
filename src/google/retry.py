# src/google/retry.py
import random
import time
from typing import Callable, TypeVar

from googleapiclient.errors import HttpError

T = TypeVar("T")

RETRY_STATUSES = {429, 500, 502, 503, 504}
RETRY_REASONS = {"rateLimitExceeded", "userRateLimitExceeded"}


def with_retries(call: Callable[[], T], *, attempts=8, base=1.0, cap=64.0) -> T:
    """
    Универсальный ретрай: экспоненциальный бэкофф с джиттером.
    attempts: макс. число попыток
    base: стартовая задержка (сек)
    cap: макс. задержка между попытками (сек)
    """
    last = None
    for i in range(attempts):
        try:
            return call()
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            reason = None
            try:
                reason = e.error_details[0]["reason"]  # Py API иногда парсит так
            except Exception:
                pass
            if (status in RETRY_STATUSES) or (reason in RETRY_REASONS):
                delay = min(cap, base * (2**i)) + random.random()
                time.sleep(delay)
                last = e
                continue
            raise
        except Exception as e:
            last = e
            delay = min(cap, base * (2**i)) + random.random()
            time.sleep(delay)
            continue
    # если не взлетело
    if isinstance(last, Exception):
        raise last
    raise RuntimeError("with_retries: exhausted without a specific exception")
