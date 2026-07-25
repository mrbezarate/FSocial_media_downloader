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
        (Regex::new(r"https?://(?:www\.)?(?:tiktok\.com/@[^/]+/video/|tiktok\.com/t/|vm\.tiktok\.com/|vt\.tiktok\.com/|v\.tiktok\.com/)[^\s]+").unwrap(), Platform::TikTok, MediaType::Video),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_youtube_urls() {
        let urls = vec![
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            "https://youtu.be/dQw4w9WgXcQ",
            "https://www.youtube.com/shorts/abcdefg",
            "https://youtube.com/playlist?list=PLxyz123"
        ];
        for url in urls {
            let m = detect(url).expect(&format!("Should detect {}", url));
            assert_eq!(m.platform, Platform::YouTube);
        }
    }

    #[test]
    fn test_tiktok_urls() {
        let urls = vec![
            "https://www.tiktok.com/@user/video/1234567890",
            "https://vm.tiktok.com/ZMxxxxxx/",
            "https://vt.tiktok.com/ZSxxxxxx/"
        ];
        for url in urls {
            let m = detect(url).expect(&format!("Should detect {}", url));
            assert_eq!(m.platform, Platform::TikTok);
        }
    }

    #[test]
    fn test_instagram_urls() {
        let urls = vec![
            "https://www.instagram.com/reel/Cxxxxxx/",
            "https://instagram.com/p/Cxxxxxx/"
        ];
        for url in urls {
            let m = detect(url).expect(&format!("Should detect {}", url));
            assert_eq!(m.platform, Platform::Instagram);
        }
    }

    #[test]
    fn test_spotify_urls() {
        let track = detect("https://open.spotify.com/track/123").unwrap();
        assert_eq!(track.platform, Platform::Spotify);
        assert_eq!(track.media_type, MediaType::Audio);

        let playlist = detect("https://open.spotify.com/playlist/123").unwrap();
        assert_eq!(playlist.platform, Platform::Spotify);
        assert_eq!(playlist.media_type, MediaType::Playlist);
    }
}
