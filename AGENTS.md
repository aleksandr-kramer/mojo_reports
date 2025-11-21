# AGENTS: технический справочник по mojo_reports

## Область действия
Файл распространяется на весь репозиторий. Следуйте указанным практикам при изменении любых файлов.

## Назначение и контекст
Mojo Reports — ETL + отчётность на Python 3.11 для сбора данных из Mojo API и Excel/Google Drive, нормализации в PostgreSQL (схемы `raw` и `core`) и генерации PDF/e-mail отчётов через Google Slides/Drive/Gmail.

- **Продакшн:** запуск задач в cron (`ops/cron/root.crontab`) через bash-скрипты; контейнеры используются только на сервере.
- **Локальная разработка без Docker:** ставьте зависимости `pip install -r requirements.txt`, настройте доступ к PostgreSQL (локальный или внешний) и секреты. Docker/Compose нужны лишь для серверной среды или если хотите поднять Postgres локально через `docker compose up -d mojo-db`.

## Обзор репозитория
- `src/settings.py` — загрузка YAML-конфига `config/config.yaml` + переменных окружения (`PG*`, `TIMEZONE`), формирует `settings` и `CONFIG`.
- `src/db.py` — `get_conn()` (psycopg2) и `advisory_lock(lock_key, wait=True)`; ключи: RAW=1001, CORE=1002, REPORTS=1003.
- `src/api/mojo_client.py` — клиент Mojo API: авторизация, пагинация/окна, ретраи.
- `src/raw/` — загрузка сырых данных: API (`attendance`, `marks/current`, `marks/final`, `schedule`, `subjects`, `work_forms`) и Excel/Drive снапшоты (`students`, `staff`, `classes`, `parents`). Оркестратор `raw_orchestrator.py` управляет init/daily/weekly-deep/backfill, фиксирует окна в `core.sync_state`, берёт окна из `config.load` и `config.api.windows`.
- `src/core/` — нормализация в схему `core` и витрины: загрузчики refs/people/classes/schedule/attendance/marks/groups. Оркестратор `core_etl.py` читает окна из `core.sync_state`, режимы `auto|init-if-empty|daily|weekly-deep|init|backfill`, обновляет чекпойнты через `core_common.py` (`get_core_checkpoint`, `set_core_checkpoint`, `validate_window_or_throw`, `json_param`, расчёт окон `chunk_window`, `compute_daily_window`).
- `src/reports/` — генерация и рассылка отчётов (coordinator daily/weekly attendance+assessment, teacher daily email-only, teacher weekly PDF блоки attendance/assessment). Использует данные `core`, конфиг `config.reports` (time zone, Google template_id/parent folders, email sender/cc, лимиты строк на слайд, шаблоны имён файлов). Запуск через `scripts/run_report_*`.
- `src/google/` — клиенты Slides/Drive/Gmail, экспорт презентаций в PDF (`slides_export.py`), отправка писем (`gmail_sender.py`, `email_worker.py`), троттлинг/ретраи (`retry.py`). Требуется сервисный аккаунт `secrets/sa.json`.
- `src/monitoring/notify_etl_failure.py` — отправка уведомлений об ошибках ETL согласно `config.monitoring.etl_failure`.
- `scripts/` — обёртки для запуска (RAW, CORE, weekly-deep, отчёты, статус ETL).
- `sql/` — первичная инициализация схем/таблиц PostgreSQL, исполняется при первом старте БД контейнера.
- `ops/cron/root.crontab` — пример расписания продакшн-кронов (RAW→CORE ежедневно, weekly-deep по воскресеньям, отчёты ночью по расписанию, бэкапы/синк медиа).

## Конфигурация и секреты
- Основной конфиг: `config/config.yaml` (эндпоинты Mojo, параметры окон, таймзоны, Google templates/папки, email-отправители, лимиты Gmail, monitoring).
- Переменные окружения (локально через shell или `.env`): `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `TIMEZONE`; для Mojo API: `MOJO_EMAIL`, `MOJO_PASSWORD`, опц. `MOJO_XSRF_TOKEN`, `MOJO_BASE_URL`; для репортов/мониторинга — Gmail/CC и прочие значения из конфига.
- Google сервисный аккаунт: `secrets/sa.json` (монтируется в контейнер `/app/secrets/sa.json`; локально просто положите файл по тому же пути).
- `.env.server` — пример переменных для контейнера; можно переопределять через `ENV_FILE` при compose/run.

## БД и чекпойнты
- Схемы `raw` и `core` + таблица `core.sync_state` (окна и время успешных синхронизаций по endpoint). Проверки на существование/пустоту таблиц встроены в оркестраторы (`to_regclass`, `EXISTS`).
- Все записи JSON в БД через `core_common.json_param` (исключает NaN).
- Идемпотентность: загрузчики делают upsert по натуральным ключам и записывают окна в `core.sync_state`; новые задачи должны соблюдать те же принципы.

## Режимы и команды запуска
**Локально без Docker**
1) `python -m venv .venv && source .venv/bin/activate`
2) `pip install -r requirements.txt`
3) Подготовьте PostgreSQL (локальный или внешний), примените `sql/*` при необходимости.
4) Экспортируйте нужные переменные окружения (см. выше) и положите `secrets/sa.json`.
5) Запускайте скрипты напрямую, напр.:
   - RAW: `bash scripts/run_raw.sh --mode auto|daily|weekly-deep|init-if-empty`
   - CORE: `bash scripts/run_core.sh --mode auto|init-if-empty|daily|weekly-deep|init|backfill [--date-from YYYY-MM-DD --date-to YYYY-MM-DD]`
   - Отчёты: `bash scripts/run_report_coord_daily.sh` и др. (требуют готовых данных core и Google cred'ов).

**Через Docker/Compose (обычно на сервере)**
- Поднять БД: `docker compose up -d mojo-db` (порт наружу закомментирован; раскомментируйте при локальной отладке).
- Выполнять задачи: `ENV_FILE=.env.server docker compose run --rm app bash scripts/run_raw.sh --mode auto` и т.п. Контейнер `app` использует смонтированный `secrets/sa.json`.

**Готовые сценарии**
- `bash scripts/run_weekly_deep.sh` — двухступенчато RAW weekly-deep → CORE weekly-deep.
- Крон-план: см. `ops/cron/root.crontab` (время Europe/Podgorica).

## Особые правила разработки
- Не заворачивайте импорты в try/except.
- Используйте существующие хелперы для окон/дат (`compute_daily_window`, `validate_window_or_throw`, `chunk_window`, `_mondays_between` и т.д.). Не выходите за «сегодня» в auto-режимах.
- Соблюдайте advisory locks при добавлении новых оркестраторов или отчётных задач (RAW=1001, CORE=1002, REPORTS=1003).
- Учитывайте таймзоны: БД хранит UTC, пользовательские отчёты используют `config.timezone`/`config.reports.timezone` для локальных вычислений.
- При добавлении config-ключей обновляйте этот файл и связанные скрипты.

## Отладка и мониторинг
- Логи крон-задач пишутся в `/var/log/mojo_reports/*.log` (см. crontab).
- При ошибках ETL можно вызвать `python -m src.monitoring.notify_etl_failure` — отправка писем согласно `config.monitoring.etl_failure`.
- Проверка БД/состояния ETL: используйте `scripts/lib_etl_status.sh`.

## Что проверить перед коммитом
- Идемпотентность загрузчиков и корректное обновление `core.sync_state` (окна не выходят за «сегодня»).
- Совместимость с существующими cron-сценариями и режимами `auto`/`init-if-empty`/`weekly-deep`.
- Наличие/актуальность секций конфигурации и секретов, отсутствие захардкоженных токенов.
