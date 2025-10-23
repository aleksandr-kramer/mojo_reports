from contextlib import contextmanager

import psycopg2

from .settings import settings


@contextmanager
def get_conn():
    conn = psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_db,
        user=settings.pg_user,
        password=settings.pg_password,
        application_name="mojo_reports",
    )
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def advisory_lock(lock_key: int, wait: bool = True):
    """
    Глобальная блокировка на период задачи.
    lock_key — произвольный int (например, RAW=1001, CORE=1002, REPORTS=1003).
    """
    with get_conn() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            if wait:
                cur.execute("SELECT pg_advisory_lock(%s);", (lock_key,))
                locked = True
            else:
                cur.execute("SELECT pg_try_advisory_lock(%s);", (lock_key,))
                locked = cur.fetchone()[0]
        try:
            if not locked:
                raise RuntimeError(f"could not acquire advisory lock {lock_key}")
            yield
        finally:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s);", (lock_key,))
