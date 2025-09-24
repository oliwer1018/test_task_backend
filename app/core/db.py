# app/core/db.py
from __future__ import annotations
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from .config import settings

engine: Engine = create_engine(settings.database_url, future=True)


def apply_schema(sql_path: str = "./sql/schema.sql") -> None:
    """Apply DDL in schema.sql. For SQLite, execute statements one-by-one."""
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    # Split on semicolons; keep non-empty statements
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    with engine.begin() as conn:
        for stmt in statements:
            conn.exec_driver_sql(stmt)
