-- Raw campaign data from test_clicks.csv
CREATE TABLE IF NOT EXISTS campaign_clicks (
date DATE NOT NULL,
campaign_id INTEGER NOT NULL,
campaign_name VARCHAR(200),
fp_feed_id VARCHAR(20),
traffic_source_id INTEGER,
clicks INTEGER,
PRIMARY KEY (date, campaign_id)
);


-- Distributed metrics (your calculated results)
CREATE TABLE IF NOT EXISTS distributed_stats (
date DATE NOT NULL,
campaign_id INTEGER NOT NULL,
campaign_name VARCHAR(200),
fp_feed_id VARCHAR(20),
traffic_source_id INTEGER,
total_searches INTEGER DEFAULT 0,
monetized_searches INTEGER DEFAULT 0,
paid_clicks INTEGER DEFAULT 0,
pub_revenue DECIMAL(12,2) DEFAULT 0,
PRIMARY KEY (date, campaign_id)
);


-- API authentication
CREATE TABLE IF NOT EXISTS publisher_keys (
api_key VARCHAR(50) PRIMARY KEY,
traffic_source_id INTEGER NOT NULL
);


-- Seed
INSERT INTO publisher_keys (api_key, traffic_source_id) VALUES
('test_key_66', 66),
('test_key_67', 67),
('test_key_68', 68)
ON CONFLICT (api_key) DO NOTHING;


-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_clicks_date_feed ON campaign_clicks (date, fp_feed_id);
CREATE INDEX IF NOT EXISTS idx_dist_ts_date ON distributed_stats (traffic_source_id, date);