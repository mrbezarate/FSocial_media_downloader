use fsocial_common::{InfoResponse, Quality};
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};

pub struct UiBuilder;

impl UiBuilder {
    pub fn build_info_message(info: &InfoResponse, max_size_mb: f64) -> String {
        if info.is_playlist {
            let safe_title = html_escape(&info.title);
            let mut dur_str = String::new();
            if let Some(dur) = info.duration_secs {
                let hours = dur / 3600;
                let minutes = (dur % 3600) / 60;
                let seconds = dur % 60;
                let d_str = if hours > 0 {
                    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
                } else {
                    format!("{:02}:{:02}", minutes, seconds)
                };
                dur_str = format!("\n⏱ <i>{}</i>", d_str);
            }
            return format!(
                "📁 <b>{}</b>\n—\n<i>Элементов: {}</i>{}\n\nФорматы для загрузки ↓",
                safe_title,
                info.playlist_count.unwrap_or(0),
                dur_str
            );
        }

        let mut lines = Vec::new();
        lines.push(format!("📹 <b>{}</b>", html_escape(&info.title)));

        if let Some(ref author) = info.uploader {
                lines.push(format!("👤 <i>{}</i>", html_escape(author)));
        }

        if let Some(dur) = info.duration_secs {
            let hours = dur / 3600;
            let minutes = (dur % 3600) / 60;
            let seconds = dur % 60;
            let d_str = if hours > 0 {
                format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
            } else {
                format!("{:02}:{:02}", minutes, seconds)
            };
            lines.push(format!("⏱ <i>{}</i>", d_str));
        }

        lines.push(String::new());

        for opt in &info.available_qualities {
            let size_mb = opt.filesize_bytes.map(|b| b / (1024 * 1024)).unwrap_or(0);
            let q_name = match opt.quality {
                Quality::Video360p => "360p",
                Quality::Video480p => "480p",
                Quality::Video720p => "720p",
                Quality::Video1080p => "1080p",
                Quality::Video1440p => "1440p",
                Quality::Video4K => "4K",
                Quality::Best => "Best",
                Quality::Audio128 => "128k",
                Quality::Audio256 => "256k",
                Quality::AudioBest => "MP3",
            };

            let speed_icon = match size_mb {
                0..=35 => "⚡️",
                36..=120 => "🚀",
                _ => "⚖️",
            };
            
            let size_str = if size_mb > 0 {
                format!("{:>4}MB", size_mb)
            } else {
                "  ~MB".to_string()
            };

            lines.push(format!("{} {:>5} | {}", speed_icon, q_name, size_str));
        }

        lines.push(String::new());
        let has_large_files = info.available_qualities.iter().any(|q| {
            q.filesize_bytes.map(|b| b as f64 / (1024.0 * 1024.0) > max_size_mb).unwrap_or(false)
        });

        if has_large_files {
            lines.push(format!("⚠️ Файлы > {} МБ не входят в ваш уровень подписки.", max_size_mb));
        }

        lines.join("\n")
    }

    pub fn build_quality_keyboard(info: &InfoResponse, short_id: &str, max_size_mb: f64) -> InlineKeyboardMarkup {
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
            let size_mb = opt.filesize_bytes.map(|b| b as f64 / (1024.0 * 1024.0)).unwrap_or(0.0);
            let mut text = match opt.quality {
                Quality::Video360p => "360p".to_string(),
                Quality::Video480p => "480p".to_string(),
                Quality::Video720p => "720p".to_string(),
                Quality::Video1080p => "1080p".to_string(),
                Quality::Video1440p => "1440p".to_string(),
                Quality::Video4K => "4K".to_string(),
                Quality::Best => "⭐ Лучшее".to_string(),
                Quality::Audio128 => "🎵 128k".to_string(),
                Quality::Audio256 => "🎵 256k".to_string(),
                Quality::AudioBest => "🎵 MP3".to_string(),
            };

            if size_mb > max_size_mb {
                text = format!("⚠️ {}", text);
            }

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
