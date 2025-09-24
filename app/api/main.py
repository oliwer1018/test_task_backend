from __future__ import annotations
from typing import List, Optional
from datetime import date, datetime
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from ..core.config import settings
from ..core.db import engine, apply_schema
from ..core.auth import traffic_source_for_key

app = FastAPI()
apply_schema()  # ensure tables on app start (fine for test/demo)


class PubStat(BaseModel):
    date: date
    campaign_id: int
    campaign_name: Optional[str]
    total_searches: int
    monetized_searches: int
    paid_clicks: int
    revenue: float
    feed_id: Optional[str]


@app.get("/pubstats", response_model=List[PubStat])
def pubstats(ts: int = Query(...), from_: str = Query(alias="from"), to: str = Query(...), key: str = Query(...)):
    ts_by_key = traffic_source_for_key(key)
    if ts_by_key is None:
        raise HTTPException(status_code=401, detail="Invalid API key")
    if ts_by_key != ts:
        raise HTTPException(status_code=403, detail="Access denied for this traffic source")

    try:
        d_from = datetime.strptime(from_, "%Y-%m-%d").date()
        d_to = datetime.strptime(to, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format, expected YYYY-MM-DD")

    if d_from > d_to:
        raise HTTPException(status_code=400, detail="from must be <= to")
    if (d_to - d_from).days > settings.max_range_days:
        raise HTTPException(status_code=400, detail=f"Max date range is {settings.max_range_days} days")

    with engine.begin() as conn:
        rows = conn.execute(text(
            """
            SELECT date, campaign_id, campaign_name,
                   total_searches, monetized_searches, paid_clicks,
                   ROUND(pub_revenue, 2) AS revenue,
                   fp_feed_id AS feed_id
            FROM distributed_stats
            WHERE traffic_source_id = :ts
              AND date >= :dfrom AND date <= :dto
              AND pub_revenue > 0
            ORDER BY date DESC, campaign_id ASC
            """
        ), {"ts": ts, "dfrom": d_from, "dto": d_to}).mappings().all()

    return [PubStat(**dict(r)) for r in rows]
