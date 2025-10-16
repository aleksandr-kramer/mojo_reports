# src/smoke_api.py
from __future__ import annotations

from datetime import date, timedelta

from src.api.mojo_client import MojoApiClient, MojoSettings


def main() -> None:
    st = MojoSettings()
    api = MojoApiClient(st)

    # авторизация
    api.login()
    print("✅ Login OK")

    # даты: сегодня и вчера (Europe/Podgorica нам сейчас не критичен на API-уровне)
    today = date.today()
    yday = today - timedelta(days=1)
    d0 = yday.isoformat()
    d1 = today.isoformat()

    # attendance
    a = api.attendance(start_date=d0, finish_date=d1)
    print(
        f"📌 attendance: total={a.get('data', {}).get('total')} items={len(a.get('data', {}).get('items', []))}"
    )

    # marks/current
    mc = api.marks_current(start_date=d0, finish_date=d1)
    print(
        f"📌 marks/current: total={mc.get('data', {}).get('total')} items={len(mc.get('data', {}).get('items', []))}"
    )

    # marks/final
    mf = api.marks_final()
    print(
        f"📌 marks/final: total={mf.get('data', {}).get('total', 0)} items={len(mf.get('data', {}).get('items', [])) if isinstance(mf.get('data', {}).get('items', []), list) else 'n/a'}"
    )

    # schedule (по сегодняшней дате)
    sch = api.schedule(search_date=d1)
    print(
        f"📌 schedule: total={sch.get('data', {}).get('total')} items={len(sch.get('data', {}).get('items', []))}"
    )

    # subjects
    sj = api.subjects()
    subj_list = sj.get("data", [])
    print(
        f"📌 subjects: count={len(subj_list) if isinstance(subj_list, list) else 'n/a'}"
    )

    # work_forms
    wf = api.work_forms()
    forms = wf.get("data", {}).get("form_list") or wf.get("data", {}).get("forms") or []
    print(f"📌 work_forms: count={len(forms)}")

    print("✅ API smoke passed")


if __name__ == "__main__":
    main()
