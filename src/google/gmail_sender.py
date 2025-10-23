import base64
import mimetypes
from email.message import EmailMessage
from typing import List, Optional, Tuple

from .clients import build_services


def _build_mime_message(
    sender: str,
    to_addrs: List[str],
    cc_addrs: Optional[List[str]],
    subject: str,
    html_body: str,
    attachment_bytes: Optional[bytes] = None,
    attachment_filename: Optional[str] = None,
) -> EmailMessage:
    """
    Собирает MIME-письмо с HTML-телом и опциональным PDF-вложением.
    """
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join([a for a in to_addrs if a])
    if cc_addrs:
        msg["Cc"] = ", ".join([a for a in cc_addrs if a])
    msg["Subject"] = subject

    # HTML тело
    msg.add_alternative(html_body, subtype="html")

    # Вложение (PDF, но поддержим общий случай по mimetypes)
    if attachment_bytes and attachment_filename:
        ctype, encoding = mimetypes.guess_type(attachment_filename)
        if ctype is None:
            ctype = "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        msg.add_attachment(
            attachment_bytes,
            maintype=maintype,
            subtype=subtype,
            filename=attachment_filename,
        )

    return msg


def send_email_with_attachment(
    sender: str,
    to_addrs: List[str],
    cc_addrs: Optional[List[str]],
    subject: str,
    html_body: str,
    attachment_bytes: Optional[bytes],
    attachment_filename: Optional[str],
) -> str:
    """
    Отправляет письмо через Gmail API с вложением.
    Возвращает message_id (если API вернул).
    """
    # Инициализируем только Gmail (другие сервисы нам тут не нужны)
    _, _, gmail = build_services()

    msg = _build_mime_message(
        sender=sender,
        to_addrs=to_addrs,
        cc_addrs=cc_addrs,
        subject=subject,
        html_body=html_body,
        attachment_bytes=attachment_bytes,
        attachment_filename=attachment_filename,
    )

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    body = {"raw": raw}

    # userId='me' работает при импёрсонации: письмо отправится от имени impersonated user.
    sent = gmail.users().messages().send(userId="me", body=body).execute()
    return sent.get("id", "")
