import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.yaml"
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f) or {}


@dataclass
class Settings:
    pg_host: str = os.getenv("PGHOST", "localhost")
    pg_port: int = int(os.getenv("PGPORT", "4507"))
    pg_db: str = os.getenv("PGDATABASE", "mojo_reports")
    pg_user: str = os.getenv("PGUSER", "mojo_user")
    pg_password: str = os.getenv("PGPASSWORD", "")
    timezone: str = os.getenv("TIMEZONE", CONFIG.get("timezone", "Europe/Podgorica"))


settings = Settings()
