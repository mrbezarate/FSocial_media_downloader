use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

// ─── Platform Detection ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Platform {
    YouTube,
    TikTok,
    Instagram,
    Spotify,
    SoundCloud,
    Pinterest,
    Unknown,
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Platform::YouTube => write!(f, "YouTube"),
            Platform::TikTok => write!(f, "TikTok"),
            Platform::Instagram => write!(f, "Instagram"),
            Platform::Spotify => write!(f, "Spotify"),
            Platform::SoundCloud => write!(f, "SoundCloud"),
            Platform::Pinterest => write!(f, "Pinterest"),
            Platform::Unknown => write!(f, "Unknown"),
        }
    }
}

// ─── Media Type ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MediaType {
    Video,
    Audio,
    Photo,
    Playlist,
}

// ─── Quality Levels ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Quality {
    Audio128,
    Audio256,
    AudioBest,
    Video360p,
    Video480p,
    Video720p,
    Video1080p,
    Video4K,
    Best,
}

impl Quality {
    /// Returns yt-dlp format selector string for this quality level
    pub fn ytdlp_format(&self) -> &str {
        match self {
            Quality::Audio128 => "bestaudio[abr<=128]/bestaudio/best",
            Quality::Audio256 => "bestaudio[abr<=256]/bestaudio/best",
            Quality::AudioBest => "bestaudio/best",
            Quality::Video360p => "bestvideo[height<=360]+bestaudio/best[height<=360]",
            Quality::Video480p => "bestvideo[height<=480]+bestaudio/best[height<=480]",
            Quality::Video720p => "bestvideo[height<=720]+bestaudio/best[height<=720]",
            Quality::Video1080p => "bestvideo[height<=1080]+bestaudio/best[height<=1080]",
            Quality::Video4K => "bestvideo[height<=2160]+bestaudio/best[height<=2160]",
            Quality::Best => "bestvideo+bestaudio/best",
        }
    }

    /// Human-readable label with emoji for Telegram inline keyboards
    pub fn display_name(&self) -> &str {
        match self {
            Quality::Audio128 => "🎵 128 kbps",
            Quality::Audio256 => "🎵 256 kbps",
            Quality::AudioBest => "🎵 Лучшее аудио",
            Quality::Video360p => "📹 360p",
            Quality::Video480p => "📹 480p",
            Quality::Video720p => "📹 720p",
            Quality::Video1080p => "📹 1080p",
            Quality::Video4K => "📹 4K",
            Quality::Best => "⭐ Лучшее качество",
        }
    }

    /// Callback data identifier for Telegram inline buttons
    pub fn callback_id(&self) -> &str {
        match self {
            Quality::Audio128 => "q_a128",
            Quality::Audio256 => "q_a256",
            Quality::AudioBest => "q_abest",
            Quality::Video360p => "q_v360",
            Quality::Video480p => "q_v480",
            Quality::Video720p => "q_v720",
            Quality::Video1080p => "q_v1080",
            Quality::Video4K => "q_v4k",
            Quality::Best => "q_best",
        }
    }

    /// Parse from callback data string
    pub fn from_callback(s: &str) -> Option<Self> {
        match s {
            "q_a128" => Some(Quality::Audio128),
            "q_a256" => Some(Quality::Audio256),
            "q_abest" => Some(Quality::AudioBest),
            "q_v360" => Some(Quality::Video360p),
            "q_v480" => Some(Quality::Video480p),
            "q_v720" => Some(Quality::Video720p),
            "q_v1080" => Some(Quality::Video1080p),
            "q_v4k" => Some(Quality::Video4K),
            "q_best" => Some(Quality::Best),
            _ => None,
        }
    }

    /// Returns the set of quality options for video content
    pub fn video_options() -> Vec<Quality> {
        vec![
            Quality::Video360p,
            Quality::Video480p,
            Quality::Video720p,
            Quality::Video1080p,
            Quality::Video4K,
            Quality::AudioBest,
            Quality::Best,
        ]
    }

    /// Returns the set of quality options for audio content
    pub fn audio_options() -> Vec<Quality> {
        vec![
            Quality::Audio128,
            Quality::Audio256,
            Quality::AudioBest,
        ]
    }

    pub fn is_audio(&self) -> bool {
        matches!(self, Quality::Audio128 | Quality::Audio256 | Quality::AudioBest)
    }
}

// ─── Download Task (Gateway → Worker via NATS) ───────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadTask {
    pub task_id: String,
    pub url: String,
    pub platform: Platform,
    pub media_type: MediaType,
    pub quality: Quality,
    pub chat_id: i64,
    pub message_id: i32,
    pub status_message_id: Option<i32>,
    pub reply_to_message_id: Option<i32>,
    pub user_id: u64,
    pub is_group: bool,
    pub spotify_meta: Option<SpotifyTrackMeta>,
    pub playlist_urls: Option<Vec<String>>,
    pub created_at: DateTime<Utc>,
}

impl DownloadTask {
    pub fn new(
        url: String,
        platform: Platform,
        media_type: MediaType,
        quality: Quality,
        chat_id: i64,
        message_id: i32,
        user_id: u64,
        is_group: bool,
    ) -> Self {
        Self {
            task_id: uuid::Uuid::new_v4().to_string(),
            url,
            platform,
            media_type,
            quality,
            chat_id,
            message_id,
            status_message_id: None,
            reply_to_message_id: None,
            user_id,
            is_group,
            spotify_meta: None,
            playlist_urls: None,
            created_at: Utc::now(),
        }
    }
}

// ─── Task Result (Worker → Gateway via NATS) ─────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: String,
    pub chat_id: i64,
    pub status_message_id: Option<i32>,
    pub reply_to_message_id: Option<i32>,
    pub is_group: bool,
    pub status: TaskStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskStatus {
    Completed {
        file_path: String,
        title: String,
        duration_secs: Option<u64>,
        performer: Option<String>,
        thumb_path: Option<String>,
        is_audio: bool,
    },
    PlaylistCompleted {
        files: Vec<(String, String, Option<u64>, Option<String>, bool)>, // path, title, duration, performer, is_audio
        playlist_title: String,
    },
    Failed {
        error: String,
        retryable: bool,
    },
    Progress {
        percent: u8,
        status_text: String,
    },
    PlaylistProgress {
        completed: u32,
        total: u32,
        status_text: String,
    },
}

// ─── Spotify Metadata ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotifyTrackMeta {
    pub title: String,
    pub artists: Vec<String>,
    pub album: String,
    pub year: Option<u32>,
    pub track_number: Option<u32>,
    pub total_tracks: Option<u32>,
    pub isrc: Option<String>,
    pub cover_url: Option<String>,
    pub duration_ms: u64,
    pub genres: Vec<String>,
}

impl SpotifyTrackMeta {
    /// Build a YouTube Music search query from Spotify metadata
    pub fn youtube_search_query(&self) -> String {
        let artist = self.artists.first().cloned().unwrap_or_default();
        format!("ytsearch1:{} - {} audio", artist, self.title)
    }

    /// Primary artist name
    pub fn primary_artist(&self) -> &str {
        self.artists.first().map(|s| s.as_str()).unwrap_or("Unknown Artist")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfoRequest {
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityOption {
    pub quality: Quality,
    pub filesize_bytes: Option<u64>,
    pub estimated_secs: Option<u64>,
    pub speed_category: String,
    pub display_label: String,
    pub full_button_label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfoResponse {
    pub title: String,
    pub uploader: Option<String>,
    pub thumbnail: Option<String>,
    pub duration_secs: Option<u64>,
    pub available_qualities: Vec<QualityOption>,
    pub is_playlist: bool,
    pub playlist_count: Option<u32>,
    pub playlist_urls: Vec<String>,
    pub error: Option<String>,
}

// ─── NATS Subjects ───────────────────────────────────────────────────────────

pub mod subjects {
    /// Subject for download tasks (Gateway → Workers)
    pub const DOWNLOAD_TASKS: &str = "tasks.download";
    /// Subject for completed/failed results (Workers → Gateway)
    pub const TASK_RESULTS: &str = "tasks.result";
    /// Subject for progress updates (Workers → Gateway)
    pub const TASK_PROGRESS: &str = "tasks.progress";
    /// JetStream stream name
    pub const STREAM_NAME: &str = "DOWNLOADS";
    /// Consumer group name for workers
    pub const WORKER_GROUP: &str = "media-workers";
    /// Dead Letter Queue subject
    pub const DLQ: &str = "tasks.dlq";
    /// Subject for info requests (Gateway → Workers, request-reply)
    pub const INFO_REQUEST: &str = "tasks.info";
}
