import argparse
from datetime import datetime

from ..settings import CONFIG
from ..google.gmail_sender import send_email_with_attachment


def _build_body(component: str, stage: str, message: str) -> str:
    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"""
    <html>
      <body>
        <p><b>Mojo Reports: ETL failure</b></p>
        <p><b>Time (UTC):</b> {now_utc}</p>
        <p><b>Component:</b> {component}</p>
        <p><b>Stage:</b> {stage}</p>
        <p><b>Message:</b> {message}</p>
      </body>
    </html>
    """


def main() -> None:
    parser = argparse.ArgumentParser(description="Send ETL failure notification email")
    parser.add_argument("--component", required=True, help="Component name (raw/core/...)")
    parser.add_argument("--stage", required=True, help="Stage name (raw_orchestrator/core_etl/...)")
    parser.add_argument("--message", required=True, help="Short error description")

    args = parser.parse_args()

    mon_cfg = CONFIG.get("monitoring", {}).get("etl_failure", {})
    if not mon_cfg.get("enabled", True):
        return

    notify_emails = mon_cfg.get("notify_emails") or []
    if not notify_emails:
        return

    sender = mon_cfg.get("sender") or notify_emails[0]
    subject_tpl = mon_cfg.get("subject") or "Mojo Reports: ETL failure"
    subject = subject_tpl

    html_body = _build_body(
        component=args.component,
        stage=args.stage,
        message=args.message,
    )

    # Вложений нет, поэтому attachment_bytes/attachment_filename = None
    send_email_with_attachment(
        sender=sender,
        to_addrs=notify_emails,
        cc_addrs=None,
        subject=subject,
        html_body=html_body,
        attachment_bytes=None,
        attachment_filename=None,
    )


if __name__ == "__main__":
    main()
