-- Production migration script
-- Run this against an existing database to upgrade schema in-place.
-- All statements are idempotent (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).
-- FIX: add label column to confirmed_training if missing
ALTER TABLE confirmed_training ADD COLUMN IF NOT EXISTS label INT;

-- Backfill from existing category column if present
UPDATE confirmed_training
SET label = CASE
    WHEN category IN ('سياسة', 'سياسه') THEN 1
    ELSE 0
END
WHERE label IS NULL;


-- ── news table additions ───────────────────────────────────────────────────
ALTER TABLE news ADD COLUMN IF NOT EXISTS template_key    TEXT;
ALTER TABLE news ADD COLUMN IF NOT EXISTS source_url      TEXT;
ALTER TABLE news ADD COLUMN IF NOT EXISTS source_category TEXT;
ALTER TABLE news ADD COLUMN IF NOT EXISTS source_label    TEXT;
ALTER TABLE news ADD COLUMN IF NOT EXISTS detected_at     TIMESTAMPTZ;
ALTER TABLE news ADD COLUMN IF NOT EXISTS scraped_at      TIMESTAMPTZ;
ALTER TABLE news ADD COLUMN IF NOT EXISTS inserted_at     TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE news ADD COLUMN IF NOT EXISTS processed_at    TIMESTAMPTZ;
ALTER TABLE news ADD COLUMN IF NOT EXISTS queued_at       TIMESTAMPTZ;

-- ── confirmed_training: add binary label column ────────────────────────────
ALTER TABLE confirmed_training ADD COLUMN IF NOT EXISTS label INT;
-- Backfill label from existing category strings
UPDATE confirmed_training
SET label = CASE
    WHEN category IN ('سياسة','سياسه') THEN 1
    ELSE 0
END
WHERE label IS NULL AND category IS NOT NULL;

-- ── news_queue additions ───────────────────────────────────────────────────
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS detected_at      TIMESTAMPTZ;
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS scraped_at       TIMESTAMPTZ;
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS inserted_at      TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS queued_at        TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS processing_at    TIMESTAMPTZ;
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS priority_score   INT DEFAULT 0;
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS instagram_status TEXT DEFAULT 'pending';
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS twitter_status   TEXT DEFAULT 'pending';
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS telegram_attempts  INT DEFAULT 0;
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS instagram_attempts INT DEFAULT 0;
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS twitter_attempts   INT DEFAULT 0;
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS facebook_attempts  INT DEFAULT 0;
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS category      TEXT;
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS template_key  TEXT;
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS source_label  TEXT;

-- Make url UNIQUE if not already (required for ON CONFLICT)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'news_queue'::regclass
          AND contype   = 'u'
          AND conname   = 'news_queue_url_key'
    ) THEN
        ALTER TABLE news_queue ADD CONSTRAINT news_queue_url_key UNIQUE (url);
    END IF;
END $$;

-- ── social_rate_log ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS social_rate_log (
    id          SERIAL PRIMARY KEY,
    platform    TEXT NOT NULL,
    sent_at     TIMESTAMPTZ DEFAULT NOW(),
    article_id  INT,
    queue_id    INT
);

-- ── scraper_health ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS scraper_health (
    id                   SERIAL PRIMARY KEY,
    category_url         TEXT UNIQUE,
    category_name        TEXT,
    last_checked         TIMESTAMPTZ DEFAULT NOW(),
    last_success         TIMESTAMPTZ,
    last_failure         TIMESTAMPTZ,
    consecutive_failures INT DEFAULT 0,
    articles_found       INT DEFAULT 0,
    status               TEXT DEFAULT 'ok'
);

-- ── new indexes ────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_news_source_url       ON news(source_url);
CREATE INDEX IF NOT EXISTS idx_news_inserted_at      ON news(inserted_at DESC);
CREATE INDEX IF NOT EXISTS idx_queue_priority_score  ON news_queue(priority_score DESC, queued_at ASC) WHERE status='pending';
CREATE INDEX IF NOT EXISTS idx_rate_log_platform     ON social_rate_log(platform, sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_scraper_health_url    ON scraper_health(category_url);
