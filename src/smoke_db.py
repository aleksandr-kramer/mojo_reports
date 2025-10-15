from .db import get_conn

with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute(
            "select current_database(), current_user, inet_server_addr(), inet_server_port(), current_setting('TimeZone');"
        )
        db, user, host, port, tz = cur.fetchone()
        print(f"DB={db} USER={user} HOST={host} PORT={port} TZ={tz}")
