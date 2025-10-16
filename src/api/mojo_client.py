# src/api/mojo_client.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import requests
import yaml
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class MojoSettings:
    def __init__(self) -> None:
        # .env — приоритет
        self.base_url = (
            os.getenv("MOJO_BASE_URL") or "https://adriatic.mojo.education/api/v1"
        )
        self.email = os.getenv("MOJO_EMAIL") or ""
        self.password = os.getenv("MOJO_PASSWORD") or ""
        self.xsrf_token = os.getenv("MOJO_XSRF_TOKEN")  # опционально
        self.timeout_sec = int(os.getenv("MOJO_TIMEOUT_SEC") or 30)
        self.default_limit = int(os.getenv("MOJO_DEFAULT_LIMIT") or 500)

        # config/config.yaml — не секретные дефолты
        try:
            with open("config/config.yaml", "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            api_cfg = (cfg or {}).get("api") or {}
            self.base_url = os.getenv("MOJO_BASE_URL") or api_cfg.get(
                "base_url", self.base_url
            )
            self.default_limit = int(
                os.getenv("MOJO_DEFAULT_LIMIT")
                or api_cfg.get("default_limit", self.default_limit)
            )
            self.timeout_sec = int(
                os.getenv("MOJO_TIMEOUT_SEC")
                or api_cfg.get("timeout_sec", self.timeout_sec)
            )
            self.retry_cfg = api_cfg.get("retry", {})
            self.windows = api_cfg.get("windows", {})
            self.department_default = api_cfg.get("department_default", 0)
        except FileNotFoundError:
            self.retry_cfg = {
                "total": 5,
                "backoff_factor": 0.5,
                "status_forcelist": [429, 500, 502, 503, 504],
            }
            self.windows = {"attendance_days_back": 2, "schedule_days_forward": 7}
            self.department_default = 0


class MojoApiClient:
    def __init__(self, settings: Optional[MojoSettings] = None) -> None:
        self.s = Session()
        self.st = settings or MojoSettings()

        # retry / backoff
        retry = Retry(
            total=int(self.st.retry_cfg.get("total", 5)),
            backoff_factor=float(self.st.retry_cfg.get("backoff_factor", 0.5)),
            status_forcelist=tuple(
                self.st.retry_cfg.get("status_forcelist", [429, 500, 502, 503, 504])
            ),
            allowed_methods=frozenset(["GET", "POST"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.s.mount("https://", adapter)
        self.s.mount("http://", adapter)

        self.s.headers.update({"accept": "application/json"})
        if self.st.xsrf_token:
            self.s.headers.update({"X-XSRF-TOKEN": self.st.xsrf_token})

        self._token: Optional[str] = None
        self._token_ts: float = 0.0  # когда получен

    # --- auth ---
    def login(self) -> None:
        url = f"{self.st.base_url.rstrip('/')}/login"
        payload = {"email": self.st.email, "password": self.st.password}
        r = self.s.post(url, json=payload, timeout=self.st.timeout_sec)
        r.raise_for_status()
        data = (
            r.json().get("data", {})
            if r.headers.get("content-type", "").startswith("application/json")
            else {}
        )
        token = data.get("access_token") or data.get("token")
        if not token:
            raise RuntimeError("Login succeeded but token not found in response.")
        self._token = token
        self._token_ts = time.time()
        self.s.headers.update({"Authorization": f"Bearer {self._token}"})

    def _authed_get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        if not self._token:
            self.login()
        url = f"{self.st.base_url.rstrip('/')}/{path.lstrip('/')}"
        r = self.s.get(url, params=params, timeout=self.st.timeout_sec)
        if r.status_code == 401:
            # пробуем перелогиниться один раз
            self.login()
            r = self.s.get(url, params=params, timeout=self.st.timeout_sec)
        r.raise_for_status()
        return r.json()

    # --- endpoints ---
    def attendance(
        self, start_date: str, finish_date: str, limit: Optional[int] = None
    ) -> Dict[str, Any]:
        return self._authed_get(
            "attendance",
            {
                "start_date": start_date,
                "finish_date": finish_date,
                "response_type": "json",
                "limit": limit or self.st.default_limit,
            },
        )

    def marks_current(
        self, start_date: str, finish_date: str, limit: Optional[int] = None
    ) -> Dict[str, Any]:
        return self._authed_get(
            "marks/current",
            {
                "start_date": start_date,
                "finish_date": finish_date,
                "response_type": "json",
                "limit": limit or self.st.default_limit,
            },
        )

    def marks_final(self, limit: Optional[int] = None) -> Dict[str, Any]:
        return self._authed_get(
            "marks/final",
            {
                "response_type": "json",
                "limit": limit or self.st.default_limit,
            },
        )

    def schedule(self, search_date: str, limit: Optional[int] = None) -> Dict[str, Any]:
        return self._authed_get(
            "schedule",
            {
                "search_date": search_date,
                "response_type": "json",
                "limit": limit or self.st.default_limit,
            },
        )

    def subjects(self) -> Dict[str, Any]:
        return self._authed_get("subjects", {"response_type": "json"})

    def work_forms(self, department: Optional[int] = None) -> Dict[str, Any]:
        return self._authed_get(
            "work_forms",
            {
                "department": (
                    department if department is not None else self.st.department_default
                )
            },
        )

    def attendance_all(self, start_date: str, finish_date: str) -> list[dict]:
        """
        Собирает ВСЕ attendance за период [start_date..finish_date], слайся по дням.
        Предполагаем, что дневной объём < self.st.default_limit (у нас 5000+).
        """
        from datetime import date, timedelta

        d0 = date.fromisoformat(start_date)
        d1 = date.fromisoformat(finish_date)
        out: list[dict] = []
        seen: set[int] = set()

        cur = d0
        while cur <= d1:
            day = cur.isoformat()
            data = self.attendance(
                start_date=day, finish_date=day, limit=self.st.default_limit
            )
            items = data.get("data", {}).get("items", [])
            for it in items:
                # страхуемся от дублей по id
                _id = it.get("id")
                if _id is None or _id in seen:
                    continue
                seen.add(_id)
                out.append(it)
            # мягкое уважение к rate limit (60/мин): ~5 запросов/сек безопасно, но не жадничаем
            # если захочешь — можно убрать/уменьшить задержку
            import time

            time.sleep(0.2)
            cur += timedelta(days=1)

        return out

    def marks_current_all(self, start_date: str, finish_date: str) -> list[dict]:
        """
        Собирает ВСЕ marks/current за период [start_date..finish_date], слайся по дням,
        чтобы не упираться в серверный лимит (5000).
        """
        import time
        from datetime import date, timedelta

        d0 = date.fromisoformat(start_date)
        d1 = date.fromisoformat(finish_date)
        out: list[dict] = []
        seen: set[int] = set()

        cur = d0
        while cur <= d1:
            day = cur.isoformat()
            data = self.marks_current(
                start_date=day, finish_date=day, limit=self.st.default_limit
            )
            items = data.get("data", {}).get("items", [])
            for it in items:
                mid = it.get("id")
                if mid is None or mid in seen:
                    continue
                seen.add(mid)
                out.append(it)
            time.sleep(0.2)  # бережём rate limit
            cur += timedelta(days=1)

        return out
