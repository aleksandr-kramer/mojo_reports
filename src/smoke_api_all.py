from __future__ import annotations

import sys
from datetime import date, timedelta

from src.api.mojo_client import MojoApiClient, MojoSettings


def main() -> None:
    st = MojoSettings()
    api = MojoApiClient(st)
    api.login()
    print("✅ Login OK")

    # диапазон берём из аргументов или месяц по умолчанию
    if len(sys.argv) == 3:
        start_date, finish_date = sys.argv[1], sys.argv[2]
    else:
        d1 = date.today()
        d0 = d1 - timedelta(days=30)
        start_date, finish_date = d0.isoformat(), d1.isoformat()

    # attendance — выкачиваем полностью (day slicing)
    att_all = api.attendance_all(start_date, finish_date)
    print(f"📌 attendance_all[{start_date}..{finish_date}]: items={len(att_all)}")

    # marks/current — одной порцией с большим limit
    mc_all = api.marks_current_all(start_date, finish_date)
    print(f"📌 marks_current_all[{start_date}..{finish_date}]: items={len(mc_all)}")

    print("✅ API full-slice smoke passed")


if __name__ == "__main__":
    main()
