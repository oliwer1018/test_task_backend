from __future__ import annotations
import os
import pandas as pd
from sqlalchemy import text

os.environ.setdefault("DATABASE_URL", "sqlite:///./test_dist.db")

from app.core.db import engine, apply_schema
from app.etl.load import import_raw, load_feeds_csv
from app.etl.distribute import run_distribution


def setup_module(_):
    apply_schema()
    # clear tables
    with engine.begin() as conn:
        conn.exec_driver_sql("DELETE FROM campaign_clicks;")
        conn.exec_driver_sql("DELETE FROM distributed_stats;")
        conn.exec_driver_sql("DELETE FROM publisher_keys;")
        conn.exec_driver_sql("INSERT INTO publisher_keys (api_key, traffic_source_id) VALUES ('test_key_66', 66);")


def test_integer_and_revenue_distribution():
    clicks = pd.DataFrame([
        {"date": "2025-01-15", "campaign_id": 101, "campaign_name": "A", "fp_feed_id": "SB100", "traffic_source_id": 66,
         "clicks": 600},
        {"date": "2025-01-15", "campaign_id": 102, "campaign_name": "B", "fp_feed_id": "SB100", "traffic_source_id": 66,
         "clicks": 400},
    ])
    feeds = pd.DataFrame([
        {"date": "2025-01-15", "fp_feed_id": "SB100", "revenue": 100.0, "searches": 1001, "monetized_searches": 801,
         "paid_clicks": 501}
    ])

    os.makedirs("./data", exist_ok=True)
    cpath, fpath = "./data/_clicks.csv", "./data/_feeds.csv"
    clicks.to_csv(cpath, index=False)
    feeds.to_csv(fpath, index=False)

    import_raw(cpath, fpath)
    run_distribution(load_feeds_csv(fpath))

    with engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT total_searches, monetized_searches, paid_clicks, pub_revenue FROM distributed_stats WHERE date='2025-01-15' ORDER BY campaign_id"))
        stats = rows.fetchall()

    # Integer-preserving totals (1001, 801, 501) split 60/40, largest remainder gives +1 to the largest fraction (campaign 101)
    (s1, m1, p1, r1), (s2, m2, p2, r2) = stats
    assert s1 + s2 == 1001
    assert m1 + m2 == 801
    assert p1 + p2 == 501

    # 75% of $100 = $75 → 60/40 == 45.00 / 30.00
    assert round(r1 + r2, 2) == 75.00
    assert {round(r1, 2), round(r2, 2)} == {45.00, 30.00}
