PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS crawls (
  crawl_id TEXT PRIMARY KEY,
  domain TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  status TEXT,
  total_urls INTEGER DEFAULT 0,
  summary_json TEXT
);
CREATE TABLE IF NOT EXISTS schedules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  domain TEXT UNIQUE,
  created_at TEXT
);
CREATE TABLE IF NOT EXISTS urls (
  crawl_id TEXT,
  url TEXT,
  status_code INTEGER,
  crawl_depth INTEGER,
  canonical TEXT,
  canonical_status TEXT,
  robots TEXT,
  title TEXT,
  title_length INTEGER,
  title_status TEXT,
  meta_description TEXT,
  meta_length INTEGER,
  meta_status TEXT,
  h1_present INTEGER,
  h1_text TEXT
);
CREATE INDEX IF NOT EXISTS idx_urls_crawl ON urls(crawl_id);

CREATE TABLE IF NOT EXISTS redirects (
  crawl_id TEXT,
  source_url TEXT,
  chain_length INTEGER,
  chain TEXT,
  final_url TEXT,
  redirect_types TEXT,
  internal_only INTEGER,
  loop INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS broken_links (
  crawl_id TEXT,
  source_url TEXT,
  anchor_text TEXT,
  direction TEXT,
  target_url TEXT,
  target_status INTEGER,
  html_context TEXT,
  first_seen TEXT,
  last_seen TEXT
);

CREATE TABLE IF NOT EXISTS images (
  crawl_id TEXT,
  url TEXT,
  img_count INTEGER,
  legacy_img_count INTEGER,
  webp_avif_count INTEGER
);

CREATE TABLE IF NOT EXISTS structured_data (
  crawl_id TEXT,
  url TEXT,
  sd_Product INTEGER,
  sd_BreadcrumbList INTEGER,
  sd_Article INTEGER,
  sd_FAQPage INTEGER,
  sd_WebSite INTEGER,
  sd_Organization INTEGER,
  sd_Offer INTEGER,
  sd_AggregateRating INTEGER,
  parse_errors INTEGER
);

-- duplicates (materialized)
CREATE TABLE IF NOT EXISTS duplicates_titles (
  crawl_id TEXT,
  title_hash TEXT,
  title_sample TEXT,
  url_count INTEGER,
  urls_sample TEXT
);
CREATE TABLE IF NOT EXISTS duplicates_meta (
  crawl_id TEXT,
  meta_hash TEXT,
  meta_sample TEXT,
  url_count INTEGER,
  urls_sample TEXT
);
