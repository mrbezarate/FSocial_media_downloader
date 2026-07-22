use crate::error::AppError;
use crate::models::Quality;
use std::env;

/// Central application configuration, loaded from environment variables.
/// Used by both Gateway and Worker services.
#[derive(Debug, Clone)]
pub struct AppConfig {
    // ─── Telegram ────────────────────────────────────────────────────
    pub teloxide_token: String,
    /// URL of the local Bot API server (e.g., "http://telegram-bot-api:8081").
    /// If None, uses the default cloud API (api.telegram.org).
    pub telegram_api_url: Option<String>,

    // ─── NATS ────────────────────────────────────────────────────────
    pub nats_url: String,

    // ─── Redis ───────────────────────────────────────────────────────
    pub redis_url: String,

    // ─── PostgreSQL ──────────────────────────────────────────────────
    pub database_url: String,

    // ─── Shared Storage (Zero-Copy Volume) ───────────────────────────
    pub shared_data_path: String,

    // ─── Spotify API (Optional) ──────────────────────────────────────
    pub spotify_client_id: Option<String>,
    pub spotify_client_secret: Option<String>,

    // ─── Proxy Pool ──────────────────────────────────────────────────
    pub proxy_list: Vec<String>,

    // ─── Default Quality ─────────────────────────────────────────────
    pub default_video_quality: Quality,
    pub default_audio_quality: Quality,

    // ─── Worker Tuning ───────────────────────────────────────────────
    pub max_concurrent_downloads: usize,
    pub ytdlp_path: String,
    pub ffmpeg_path: String,
    pub cookies_path: Option<String>,
}

impl AppConfig {
    /// Load configuration from environment variables.
    /// Required: TELOXIDE_TOKEN
    /// All other values have sensible defaults for Docker Compose deployment.
    pub fn from_env() -> Result<Self, AppError> {
        Ok(Self {
            teloxide_token: env::var("TELOXIDE_TOKEN")
                .map_err(|_| AppError::Config("TELOXIDE_TOKEN is required but not set".into()))?,

            telegram_api_url: env::var("TELEGRAM_API_URL").ok(),

            nats_url: env::var("NATS_URL")
                .unwrap_or_else(|_| "nats://nats:4222".into()),

            redis_url: env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://redis:6379".into()),

            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://fsocial:fsocial@postgres:5432/fsocial".into()),

            shared_data_path: env::var("SHARED_DATA_PATH")
                .unwrap_or_else(|_| "/shared_data".into()),

            spotify_client_id: env::var("SPOTIFY_CLIENT_ID").ok(),
            spotify_client_secret: env::var("SPOTIFY_CLIENT_SECRET").ok(),

            proxy_list: env::var("PROXY_LIST")
                .map(|s| s.split(',').map(|p| p.trim().to_string()).filter(|p| !p.is_empty()).collect())
                .unwrap_or_default(),

            default_video_quality: Quality::Video720p,
            default_audio_quality: Quality::Audio256,

            max_concurrent_downloads: env::var("MAX_CONCURRENT_DOWNLOADS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10),

            ytdlp_path: env::var("YTDLP_PATH")
                .unwrap_or_else(|_| "yt-dlp".into()),

            ffmpeg_path: env::var("FFMPEG_PATH")
                .unwrap_or_else(|_| "ffmpeg".into()),

            cookies_path: env::var("COOKIES_PATH").ok().filter(|s| !s.is_empty()),
        })
    }

    /// Check if Spotify integration is configured
    pub fn spotify_enabled(&self) -> bool {
        self.spotify_client_id.is_some() && self.spotify_client_secret.is_some()
    }

    /// Check if proxy pool is configured
    pub fn proxies_enabled(&self) -> bool {
        !self.proxy_list.is_empty()
    }

    /// Check if local Bot API server is configured
    pub fn is_local_api(&self) -> bool {
        self.telegram_api_url.is_some()
    }
}
