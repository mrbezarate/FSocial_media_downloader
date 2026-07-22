-- ═══════════════════════════════════════════════════════════════════════════════
-- FSocial Media Downloader — Database Schema
-- PostgreSQL 16
-- ═══════════════════════════════════════════════════════════════════════════════

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ─── Users ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id                  BIGINT PRIMARY KEY,              -- Telegram user ID
    username            VARCHAR(255),
    first_name          VARCHAR(255),
    language_code       VARCHAR(10) DEFAULT 'ru',
    default_video_quality VARCHAR(20) DEFAULT 'Video720p',
    default_audio_quality VARCHAR(20) DEFAULT 'Audio256',
    total_downloads     BIGINT DEFAULT 0,
    total_bytes         BIGINT DEFAULT 0,
    is_blocked          BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Download History ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS download_history (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id     BIGINT REFERENCES users(id) ON DELETE CASCADE,
    chat_id     BIGINT NOT NULL,
    url         TEXT NOT NULL,
    platform    VARCHAR(50) NOT NULL,
    media_type  VARCHAR(20) NOT NULL,
    quality     VARCHAR(20) NOT NULL,
    title       TEXT,
    file_size   BIGINT,
    duration_s  INTEGER,
    status      VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending, completed, failed
    error_msg   TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_download_history_user_id ON download_history(user_id);
CREATE INDEX IF NOT EXISTS idx_download_history_created_at ON download_history(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_download_history_url ON download_history(url);

-- ─── Statistics (daily aggregation) ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_stats (
    date            DATE PRIMARY KEY DEFAULT CURRENT_DATE,
    total_requests  BIGINT DEFAULT 0,
    total_downloads BIGINT DEFAULT 0,
    total_failures  BIGINT DEFAULT 0,
    total_bytes     BIGINT DEFAULT 0,
    unique_users    BIGINT DEFAULT 0,
    youtube_count   BIGINT DEFAULT 0,
    tiktok_count    BIGINT DEFAULT 0,
    instagram_count BIGINT DEFAULT 0,
    spotify_count   BIGINT DEFAULT 0,
    other_count     BIGINT DEFAULT 0
);

-- ─── Update timestamp trigger ──────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();
