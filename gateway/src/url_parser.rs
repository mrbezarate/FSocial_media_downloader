use fsocial_common::{MediaType, Platform};
use regex::Regex;
use std::sync::LazyLock;
use teloxide::prelude::*;

pub struct UrlMatch {
    pub url: String,
    pub platform: Platform,
    pub media_type: MediaType,
}

static URL_PATTERNS: LazyLock<Vec<(Regex, Platform, MediaType)>> = LazyLock::new(|| {
    vec![
        (Regex::new(r"https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/|youtube\.com/live/|youtube\.com/playlist\?list=)[^\s]+").unwrap(), Platform::YouTube, MediaType::Video),
        (Regex::new(r"https?://(?:www\.)?(?:tiktok\.com/@[^/]+/video/|vm\.tiktok\.com/)[^\s]+").unwrap(), Platform::TikTok, MediaType::Video),
        (Regex::new(r"https?://(?:www\.)?instagram\.com/(?:reel|p)/[^\s]+").unwrap(), Platform::Instagram, MediaType::Video),
        (Regex::new(r"https?://(?:open\.)?spotify\.com/track/[^\s]+").unwrap(), Platform::Spotify, MediaType::Audio),
        (Regex::new(r"https?://(?:open\.)?spotify\.com/(?:album|playlist)/[^\s]+").unwrap(), Platform::Spotify, MediaType::Playlist),
        (Regex::new(r"https?://(?:www\.)?soundcloud\.com/[^\s]+").unwrap(), Platform::SoundCloud, MediaType::Audio),
        (Regex::new(r"https?://(?:www\.|pin\.)?(?:pinterest\.com/(?:pin|video)/|pin\.it/)[^\s]+").unwrap(), Platform::Pinterest, MediaType::Video),
    ]
});

pub fn detect(text: &str) -> Option<UrlMatch> {
    for (regex, platform, default_media_type) in URL_PATTERNS.iter() {
        if let Some(m) = regex.find(text) {
            return Some(UrlMatch {
                url: m.as_str().to_string(),
                platform: platform.clone(),
                media_type: default_media_type.clone(),
            });
        }
    }
    None
}

pub fn contains_url(msg: Message) -> bool {
    if let Some(text) = msg.text().or(msg.caption()) {
        return msg.chat.is_private() || text.contains("http://") || text.contains("https://") || text.contains("www.");
    }
    false
}
