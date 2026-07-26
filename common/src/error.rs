use thiserror::Error;

/// Unified error type for all application components.
/// Each variant maps to a specific subsystem failure domain.
#[derive(Error, Debug)]
pub enum AppError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("NATS error: {0}")]
    Nats(String),

    #[error("Download error: {0}")]
    Download(String),

    #[error("yt-dlp error (exit={exit_code}): {message}")]
    YtDlp { message: String, exit_code: i32 },

    #[error("FFmpeg error: {0}")]
    FFmpeg(String),

    #[error("Spotify API error: {0}")]
    Spotify(String),

    #[error("Audio tagging error: {0}")]
    Tagging(String),

    #[error("Redis error: {0}")]
    Redis(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Telegram API error: {0}")]
    Telegram(String),

    #[error("Proxy error: all proxies exhausted after {attempts} attempts")]
    ProxyExhausted { attempts: u32 },

    #[error("Cache error: {0}")]
    Cache(String),

    #[error("Rate limited, retry after {retry_after_secs}s")]
    RateLimited { retry_after_secs: u64 },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("HTTP request error: {0}")]
    Http(String),

    #[error("Platform {platform} returned {status}: {message}")]
    PlatformBlock {
        platform: String,
        status: u16,
        message: String,
    },
}

impl AppError {
    /// Whether this error is transient and the operation should be retried
    pub fn is_retryable(&self) -> bool {
        match self {
            AppError::Nats(_)
            | AppError::RateLimited { .. }
            | AppError::ProxyExhausted { .. }
            | AppError::Http(_)
            | AppError::PlatformBlock { .. } => true,
            AppError::YtDlp { message, .. } => {
                let msg_lower = message.to_lowercase();
                msg_lower.contains("rehydration")
                    || msg_lower.contains("blocked")
                    || msg_lower.contains("captcha")
                    || msg_lower.contains("connection")
                    || msg_lower.contains("timeout")
            }
            _ => false,
        }
    }
}
