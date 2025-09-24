from __future__ import annotations
import pandas as pd
from sqlalchemy import text
from . import distribute  # for type reuse if needed
from ..core.db import engine, apply_schema

UPSERT_CLICKS = text(
    """
    INSERT INTO campaign_clicks (date, campaign_id, campaign_name, fp_feed_id, traffic_source_id, clicks)
    VALUES (:date, :campaign_id, :campaign_name, :fp_feed_id, :traffic_source_id, :clicks)
    ON CONFLICT (date, campaign_id) DO UPDATE SET
      campaign_name = EXCLUDED.campaign_name,
      fp_feed_id = EXCLUDED.fp_feed_id,
      traffic_source_id = EXCLUDED.traffic_source_id,
      clicks = EXCLUDED.clicks
    """
)


def load_clicks_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date", "campaign_id"]).copy()
    df["campaign_id"] = df["campaign_id"].astype(int)
    df["clicks"] = pd.to_numeric(df["clicks"], errors="coerce").fillna(0).astype(int)
    # Exclude negatives; keep zeros
    df = df[df["clicks"] >= 0]
    return df


def load_feeds_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date", "fp_feed_id"]).copy()
    # dedup last occurrence
    df = df.drop_duplicates(subset=["date", "fp_feed_id"], keep="last")
    # normalize numeric columns
    for col in ["revenue", "searches", "monetized_searches", "paid_clicks"]:
        if col in df.columns:
            if col == "revenue":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df


def import_raw(clicks_csv: str, feeds_csv: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    apply_schema()
    clicks = load_clicks_csv(clicks_csv)
    feeds = load_feeds_csv(feeds_csv)
    with engine.begin() as conn:
        for _, r in clicks.iterrows():
            conn.execute(UPSERT_CLICKS, {
                "date": r["date"],
                "campaign_id": int(r["campaign_id"]),
                "campaign_name": r.get("campaign_name"),
                "fp_feed_id": r.get("fp_feed_id"),
                "traffic_source_id": int(r.get("traffic_source_id") or 0),
                "clicks": int(r["clicks"]),
            })
    return clicks, feeds
