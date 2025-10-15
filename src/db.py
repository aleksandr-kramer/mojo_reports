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
        application_name="mojo_reports_dev",
    )
    try:
        yield conn
    finally:
        conn.close()
