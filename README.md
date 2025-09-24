# Searchbarista Backend Test — Implementation

This project implements the assignment: a small ETL pipeline that distributes feed provider revenue to publisher campaigns, and an API to expose the results.

---

## 🚀 Features
- **Two‑table design**: raw `campaign_clicks` from CSV, processed `distributed_stats` with distributed metrics.
- **Integer conservation**: searches, monetized searches, paid clicks distributed using Largest Remainder Method → totals always match.
- **Revenue share**: 75% publisher share with cent‑level rounding preserved.
- **Edge cases handled**: duplicates, zero/negative clicks, case sensitivity.
- **API**: `/pubstats` with authentication, date range validation, sorting and filtering.
- **Idempotent**: re‑running ETL overwrites safely.

---

## 🛠 Setup

### 1. Install dependencies
```bash
python -m venv .venv && source .venv/bin/activate   # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
```

### 2. Configure environment
Copy the example env and edit if needed:
```bash
cp .env.example .env
```

### 3. Initialize database and run ETL

This step prepares the database and loads the provided CSV files (`test_clicks.csv` and `test_feeds.csv`) into it, then runs the distribution logic.

#### Quick start (recommended)
Run the helper script:
```bash
python process.py
```

## 🌐 API Usage

### Start server
```bash
uvicorn app.api.main:app --reload --port 8000
```

### Endpoint
```
GET /pubstats?ts={traffic_source_id}&from={YYYY-MM-DD}&to={YYYY-MM-DD}&key={api_key}

```

### Example
```
curl "http://localhost:8000/pubstats?ts=66&from=2025-01-15&to=2025-01-20&key=test_key_66"
```


### Response
```
[
  {
    "date": "2025-01-15",
    "campaign_id": 101,
    "campaign_name": "US_Search_Mobile_1",
    "total_searches": 600,
    "monetized_searches": 480,
    "paid_clicks": 300,
    "revenue": 45.0,
    "feed_id": "SB100"
  }
]
```

### Rules
- Authentication required: API key must exist and match `ts`.

- Date range must not exceed 90 days.

- Only returns rows where `pub_revenue > 0`.

- Results are sorted by `date DESC`, then `campaign_id ASC`.