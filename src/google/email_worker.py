# src/google/email_worker.py
import time
from datetime import datetime, timedelta
from typing import List, Optional

from googleapiclient.errors import HttpError

from ..db import get_conn
from ..settings import CONFIG
from .gmail_sender import send_email_with_attachment
from .retry import with_retries

RL = CONFIG.get("google", {}).get("rate_limits", {}).get("gmail", {})
MAX_PER_HOUR = int(RL.get("max_messages_per_hour", 90))
BURST = int(RL.get("burst", 10))
MIN_GAP = int(RL.get("min_seconds_between_sends", 40))
MAX_ERR = int(RL.get("max_consecutive_errors", 5))


def _fetch_pending_batch(limit: int = BURST):
    """
    Атомарно выбирает лимит 'pending' писем, помечая их как 'processing', и возвращает их к отправке.
    Исключает гонки при нескольких воркерах/рестартах.
    """
    q = """
    WITH cte AS (
        SELECT id
        FROM rep.email_queue
        WHERE status = 'pending'
        ORDER BY created_at
        FOR UPDATE SKIP LOCKED
        LIMIT %s
    )
    UPDATE rep.email_queue q
    SET status = 'processing'
    FROM cte
    WHERE q.id = cte.id
    RETURNING q.id, q.campaign_id, q.recipient_email, q.subject, q.html_body,
              q.attachment_bytes, q.attachment_name, q.try_count;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(q, (limit,))
        rows = cur.fetchall()
        conn.commit()
    return rows


def _mark_sent(id_: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE rep.email_queue SET status='sent', sent_at=now() WHERE id=%s",
            (id_,),
        )
        conn.commit()


def _mark_error(id_: int, err: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE rep.email_queue SET status='error', error_msg=%s, try_count=try_count+1 WHERE id=%s",
            (
                err,
                id_,
            ),
        )
        conn.commit()


def _mark_processing_to_pending(id_: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE rep.email_queue SET status='pending' WHERE id=%s AND status='processing'",
            (id_,),
        )
        conn.commit()


def _bump_try(id_: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE rep.email_queue SET try_count=try_count+1 WHERE id=%s", (id_,)
        )
        conn.commit()


def _count_sent_last_hour() -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM rep.email_queue WHERE status='sent' AND sent_at >= now() - interval '1 hour'"
        )
        return cur.fetchone()[0]


def run_forever(sender: str):
    consecutive_errors = 0
    while True:
        # лимит на час
        if _count_sent_last_hour() >= MAX_PER_HOUR:
            time.sleep(60)  # ждём минуту и проверяем снова
            continue

        batch = _fetch_pending_batch()
        if not batch:
            time.sleep(15)  # пусто — спим
            continue

        for row in batch:
            (id_, campaign, rcpt, subj, html, att_bytes, att_name, try_count) = row

            def _send_one():
                return send_email_with_attachment(
                    sender=sender,
                    to_addrs=[rcpt],
                    cc_addrs=None,
                    subject=subj,
                    html_body=html,
                    attachment_bytes=att_bytes,
                    attachment_filename=att_name,
                )

            # допустим максимум попыток по одному письму (чтобы не залипало навсегда)
            MAX_TRIES_PER_MESSAGE = 8

            try:
                # защитим вызов ретраями по сетевым/квотным ошибкам
                with_retries(_send_one, attempts=8, base=1.0, cap=64.0)
                _mark_sent(id_)
            except HttpError as e:
                _bump_try(id_)
                # перманентные ошибки (напр., 400 invalidArgument) помечаем 'error' сразу
                status = getattr(e.resp, "status", None)
                if status and int(status) in (
                    400,
                    401,
                    403,
                ):  # 403 может быть и квотным; см. ниже
                    # если 403 — возможно квоты. Можно вернуть в pending, если try_count < порога
                    if int(status) == 403 and (try_count + 1) < MAX_TRIES_PER_MESSAGE:
                        _mark_processing_to_pending(id_)
                    else:
                        _mark_error(id_, f"HttpError {status}: {e}")
                else:
                    # неизвестно — считаем временной, вернём в pending
                    if (try_count + 1) >= MAX_TRIES_PER_MESSAGE:
                        _mark_error(id_, f"HttpError {status}: {e}")
                    else:
                        _mark_processing_to_pending(id_)
            except Exception as e:
                _bump_try(id_)
                if (try_count + 1) >= MAX_TRIES_PER_MESSAGE:
                    _mark_error(id_, f"{type(e).__name__}: {e}")
                else:
                    _mark_processing_to_pending(id_)

            # пауза между письмами
            time.sleep(MIN_GAP)
