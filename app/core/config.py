from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./local.db")
    clicks_csv: str = os.getenv("CLICKS_CSV", "./data/test_clicks.csv")
    feeds_csv: str = os.getenv("FEEDS_CSV", "./data/test_feeds.csv")
    port: int = int(os.getenv("PORT", "8000"))
    max_range_days: int = 90


settings = Settings()
