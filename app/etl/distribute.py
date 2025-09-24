from __future__ import annotations
from typing import List
import pandas as pd
from sqlalchemy import text
from ..core.db import engine

UPSERT_DISTRIBUTED = text(
    """
    INSERT INTO distributed_stats (
        date, campaign_id, campaign_name, fp_feed_id, traffic_source_id,
        total_searches, monetized_searches, paid_clicks, pub_revenue
    ) VALUES (
        :date, :campaign_id, :campaign_name, :fp_feed_id, :traffic_source_id,
        :total_searches, :monetized_searches, :paid_clicks, :pub_revenue
    )
    ON CONFLICT (date, campaign_id) DO UPDATE SET
      campaign_name = EXCLUDED.campaign_name,
      fp_feed_id = EXCLUDED.fp_feed_id,
      traffic_source_id = EXCLUDED.traffic_source_id,
      total_searches = EXCLUDED.total_searches,
      monetized_searches = EXCLUDED.monetized_searches,
      paid_clicks = EXCLUDED.paid_clicks,
      pub_revenue = EXCLUDED.pub_revenue
    """
)


# ---- Largest Remainder for integers ----
def distribute_integer(total: int, weights: List[float]) -> List[int]:
    if total is None:
        total = 0
    if total <= 0 or not weights:
        return [0 for _ in weights]
    norm = [max(0.0, float(w)) for w in weights]
    s = sum(norm)
    if s == 0:
        return [0 for _ in weights]
    norm = [w / s for w in norm]
    floors = [int(total * w) for w in norm]
    remainder = int(total - sum(floors))
    if remainder > 0:
        fracs = [((total * w) - int(total * w)) for w in norm]
        order = sorted(range(len(norm)), key=lambda i: fracs[i], reverse=True)
        for i in order[:remainder]:
            floors[i] += 1
    return floors


# ---- Largest Remainder for cents (two decimals) ----
def distribute_money(amount_total: float, weights: List[float]) -> List[float]:
    cents_total = int(round(amount_total * 100))
    norm = [max(0.0, float(w)) for w in weights]
    s = sum(norm)
    if s == 0 or cents_total <= 0:
        return [0.0 for _ in weights]
    norm = [w / s for w in norm]
    base = [int((cents_total * w)) for w in norm]
    remainder = cents_total - sum(base)
    if remainder > 0:
        fracs = [((cents_total * w) - int(cents_total * w)) for w in norm]
        order = sorted(range(len(norm)), key=lambda i: fracs[i], reverse=True)
        for i in order[:remainder]:
            base[i] += 1
    return [b / 100.0 for b in base]


def run_distribution(feeds: pd.DataFrame) -> None:
    with engine.begin() as conn:
        for (date, fp_feed_id), grp in feeds.groupby(["date", "fp_feed_id"], sort=False):
            feed = grp.iloc[-1]  # last occurrence per spec
            rows = conn.execute(text(
                """
                SELECT campaign_id, campaign_name, fp_feed_id, traffic_source_id, clicks
                FROM campaign_clicks
                WHERE date = :date AND fp_feed_id = :fp
                """
            ), {"date": date, "fp": fp_feed_id}).mappings().all()

            if not rows:
                # No matching campaigns — skip per spec
                continue

            clicks = [max(0, int(r["clicks"])) for r in rows]
            total_clicks = sum(clicks)

            if total_clicks == 0:
                # all zero → write zero metrics
                for r in rows:
                    conn.execute(UPSERT_DISTRIBUTED, {
                        "date": date,
                        "campaign_id": int(r["campaign_id"]),
                        "campaign_name": r["campaign_name"],
                        "fp_feed_id": fp_feed_id,
                        "traffic_source_id": int(r["traffic_source_id"] or 0),
                        "total_searches": 0,
                        "monetized_searches": 0,
                        "paid_clicks": 0,
                        "pub_revenue": 0.0,
                    })
                continue

            weights = [c / total_clicks for c in clicks]

            total_searches = int(feed.get("searches", 0) or 0)
            monetized_searches = int(feed.get("monetized_searches", 0) or 0)
            paid_clicks = int(feed.get("paid_clicks", 0) or 0)
            revenue_total = float(feed.get("revenue", 0.0) or 0.0) * 0.75  # 75% publisher share

            dist_searches = distribute_integer(total_searches, weights)
            dist_msearches = distribute_integer(monetized_searches, weights)
            dist_paid = distribute_integer(paid_clicks, weights)
            dist_revenue = distribute_money(revenue_total, weights)

            for i, r in enumerate(rows):
                conn.execute(UPSERT_DISTRIBUTED, {
                    "date": date,
                    "campaign_id": int(r["campaign_id"]),
                    "campaign_name": r["campaign_name"],
                    "fp_feed_id": fp_feed_id,
                    "traffic_source_id": int(r["traffic_source_id"] or 0),
                    "total_searches": dist_searches[i],
                    "monetized_searches": dist_msearches[i],
                    "paid_clicks": dist_paid[i],
                    "pub_revenue": dist_revenue[i],
                })
