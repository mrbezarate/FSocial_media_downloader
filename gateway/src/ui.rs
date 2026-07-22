use fsocial_common::{InfoResponse, Quality};
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};

pub struct UiBuilder;

impl UiBuilder {
    pub fn build_info_message(info: &InfoResponse) -> String {
        if info.is_playlist {
            let safe_title = html_escape(&info.title);
            return format!(
                "📁 <b>{}</b> →\n\n<i>Всего элементов: {}</i>\n\nФорматы для скачивания ↓",
                safe_title,
                info.playlist_count.unwrap_or(0)
            );
        }

        let mut lines = Vec::new();
        lines.push(format!("📹 <b>{}</b> →", html_escape(&info.title)));

        if let Some(ref author) = info.uploader {
            if !author.is_empty() {
                lines.push(format!("👤 <b>{}</b> →", html_escape(author)));
            }
        }

        lines.push(String::new());

        for opt in &info.available_qualities {
            let size_mb = opt.filesize_bytes.map(|b| b / (1024 * 1024)).unwrap_or(0);
            let speed_icon = match size_mb {
                0..=35 => "⚡️",
                36..=120 => "🚀",
                _ => "⚖️",
            };
            let q_name = match opt.quality {
                Quality::Video360p => "360p",
                Quality::Video480p => "480p",
                Quality::Video720p => "720p",
                Quality::Video1080p => "1080p",
                Quality::Video4K => "4K",
                Quality::Best => "Best",
                Quality::Audio128 => "128k",
                Quality::Audio256 => "256k",
                Quality::AudioBest => "MP3",
            };

            let size_str = if size_mb > 0 {
                format!("{:>4}MB", size_mb)
            } else {
                "  ~MB".to_string()
            };

            lines.push(format!("{} {:>5}: {}", speed_icon, q_name, size_str));
        }

        lines.push(String::new());
        lines.push("Форматы для скачивания ↓".to_string());

        lines.join("\n")
    }

    pub fn build_quality_keyboard(info: &InfoResponse, short_id: &str) -> InlineKeyboardMarkup {
        if info.is_playlist {
            let mut default_q = Quality::Video720p;
            if info.available_qualities.iter().any(|q| q.quality.is_audio()) && 
               !info.available_qualities.iter().any(|q| !q.quality.is_audio()) {
                default_q = Quality::AudioBest;
            }
            let btn = InlineKeyboardButton::callback(
                format!("📥 Скачать плейлист ({})", info.playlist_count.unwrap_or(0)),
                format!("{}|{}", default_q.callback_id(), short_id),
            );
            return InlineKeyboardMarkup::new(vec![vec![btn]]);
        }

        let mut video_btns = Vec::new();
        let mut audio_btns = Vec::new();

        for opt in &info.available_qualities {
            let text = match opt.quality {
                Quality::Video360p => "360p".to_string(),
                Quality::Video480p => "480p".to_string(),
                Quality::Video720p => "720p".to_string(),
                Quality::Video1080p => "1080p".to_string(),
                Quality::Video4K => "4K".to_string(),
                Quality::Best => "⭐ Best".to_string(),
                Quality::Audio128 => "MP3 128k".to_string(),
                Quality::Audio256 => "MP3 256k".to_string(),
                Quality::AudioBest => "🎵 MP3".to_string(),
            };

            let btn = InlineKeyboardButton::callback(
                text,
                format!("{}|{}", opt.quality.callback_id(), short_id),
            );

            if opt.quality.is_audio() {
                audio_btns.push(btn);
            } else {
                video_btns.push(btn);
            }
        }

        let mut rows = Vec::new();

        for chunk in video_btns.chunks(3) {
            rows.push(chunk.to_vec());
        }

        for chunk in audio_btns.chunks(2) {
            rows.push(chunk.to_vec());
        }

        InlineKeyboardMarkup::new(rows)
    }
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}
