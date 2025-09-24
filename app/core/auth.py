from __future__ import annotations
from typing import Optional
from sqlalchemy import text
from .db import engine


def traffic_source_for_key(api_key: str) -> Optional[int]:
    with engine.begin() as conn:
        row = conn.execute(text("SELECT traffic_source_id FROM publisher_keys WHERE api_key = :k"),
                           {"k": api_key}).first()
        return int(row[0]) if row else None
