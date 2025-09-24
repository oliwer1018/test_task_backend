from __future__ import annotations
import os
import pandas as pd
from fastapi.testclient import TestClient
from sqlalchemy import text

os.environ.setdefault("DATABASE_URL", "sqlite:///./test_api.db")

from app.core.db import engine, apply_schema
from app.etl.load import import_raw, load_feeds_csv
from app.etl.distribute import run_distribution
from app.api.main import app

client = TestClient(app)


def setup_module(_):
    apply_schema()
    with engine.begin() as conn:
        conn.exec_driver_sql("DELETE FROM campaign_clicks;")
        conn.exec_driver_sql("DELETE FROM distributed_stats;")
        conn.exec_driver_sql("DELETE FROM publisher_keys;")
        conn.exec_driver_sql("INSERT INTO publisher_keys (api_key, traffic_source_id) VALUES ('test_key_66', 66);")

    # Seed small dataset
    clicks = pd.DataFrame([
        {"date": "2025-01-15", "campaign_id": 101, "campaign_name": "A", "fp_feed_id": "SB100", "traffic_source_id": 66,
         "clicks": 600},
        {"date": "2025-01-15", "campaign_id": 102, "campaign_name": "B", "fp_feed_id": "SB100", "traffic_source_id": 66,
         "clicks": 400},
    ])
    feeds = pd.DataFrame([
        {"date": "2025-01-15", "fp_feed_id": "SB100", "revenue": 100.0, "searches": 1000, "monetized_searches": 800,
         "paid_clicks": 500}
    ])
    os.makedirs("./data", exist_ok=True)
    cpath, fpath = "./data/_clicks_api.csv", "./data/_feeds_api.csv"
    clicks.to_csv(cpath, index=False)
    feeds.to_csv(fpath, index=False)

    import_raw(cpath, fpath)
    run_distribution(load_feeds_csv(fpath))


def test_pubstats_success():
    r = client.get("/pubstats", params={"ts": 66, "from": "2025-01-15", "to": "2025-01-15", "key": "test_key_66"})
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    assert data[0]["date"] == "2025-01-15"
    # sort by date desc, campaign asc
    assert [row["campaign_id"] for row in data] == [101, 102]
    assert round(sum(row["revenue"] for row in data), 2) == 75.00


def test_auth_and_range_errors():
    # invalid key
    r = client.get("/pubstats", params={"ts": 66, "from": "2025-01-15", "to": "2025-01-15", "key": "wrong"})
    assert r.status_code == 401

    # mismatched ts
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO publisher_keys (api_key, traffic_source_id) VALUES ('test_key_67', 67)"))
    r = client.get("/pubstats", params={"ts": 66, "from": "2025-01-15", "to": "2025-01-15", "key": "test_key_67"})
    assert r.status_code == 403

    # > max range
    r = client.get("/pubstats", params={"ts": 66, "from": "2025-01-01", "to": "2025-04-05", "key": "test_key_66"})
    assert r.status_code == 400
