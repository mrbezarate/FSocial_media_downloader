use fsocial_common::{AppConfig, AppError, Quality};
use serde_json::Value;
use std::process::Stdio;
use tokio::process::Command;
use tracing::{error, info, instrument};

#[derive(Debug, Clone)]
pub struct YtDlpOutput {
    pub file_path: String,
    pub title: String,
    pub duration: Option<u64>,
    pub thumbnail: Option<String>,
    pub uploader: Option<String>,
}

use tokio::io::{AsyncBufReadExt, BufReader};

#[instrument(skip(config, progress_tx))]
pub async fn download(
    config: &AppConfig,
    url: &str,
    quality: &Quality,
    output_dir: &str,
    proxy: Option<&str>,
    progress_tx: Option<tokio::sync::mpsc::Sender<fsocial_common::ProgressEvent>>,
    is_premium: bool,
) -> Result<YtDlpOutput, AppError> {
    let uuid = uuid::Uuid::new_v4().to_string();
    let prefix_guard = fsocial_common::file_guard::PrefixGuard::new(output_dir.to_string(), uuid.clone());
    let mut cmd = Command::new(&config.ytdlp_path);

    cmd.arg("--no-warnings")
        .arg("-f")
        .arg(quality.ytdlp_format())
        .arg("-o")
        .arg(format!("{}/{}.%(ext)s", output_dir, uuid))
        .arg("--newline")
        .arg("--write-info-json")
        .arg("--no-playlist")
        .arg("-N")
        .arg("4") // 4 concurrent fragments to speed up downloads
        .arg("--embed-thumbnail")
        .arg("--embed-metadata")
        .arg("--ffmpeg-location")
        .arg(&config.ffmpeg_path);

    if quality.is_audio() {
        cmd.arg("--extract-audio").arg("--audio-format").arg("mp3");
    } else {
        cmd.arg("--merge-output-format").arg("mp4");
    }

    if let Some(p) = proxy {
        cmd.arg("--proxy").arg(p);
    }

    if let Some(ref cookies) = config.cookies_path {
        cmd.arg("--cookies").arg(cookies);
    }

    if !is_premium {
        cmd.arg("--limit-rate").arg("3M");
    }

    cmd.arg(url);

    #[cfg(unix)]
    use std::os::unix::process::CommandExt;

    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            libc::setpgid(0, 0);
            Ok(())
        });
    }

    info!("Running yt-dlp command: {:?}", cmd);
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    struct KillProcessGroupOnDrop(Option<u32>);
    impl Drop for KillProcessGroupOnDrop {
        fn drop(&mut self) {
            if let Some(pid) = self.0 {
                #[cfg(unix)]
                unsafe {
                    libc::kill(-(pid as i32), libc::SIGKILL);
                }
            }
        }
    }
    let mut _kill_guard = KillProcessGroupOnDrop(child.id());

    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout).lines();

    let stderr_stream = child.stderr.take().unwrap();
    let mut stderr_reader = BufReader::new(stderr_stream).lines();

    let error_log = std::sync::Arc::new(tokio::sync::Mutex::new(String::new()));
    let error_log_clone = error_log.clone();

    tokio::spawn(async move {
        while let Ok(Some(line)) = stderr_reader.next_line().await {
            let mut lock = error_log_clone.lock().await;
            lock.push_str(&line);
            lock.push('\n');
        }
    });

    while let Ok(Some(line)) = reader.next_line().await {
        // yt-dlp with -N (multi-thread) may output multiple progress updates
        // joined by \r in a single line. Split to catch each one.
        for part in line.split('\r') {
            let trimmed = part.trim();
            if trimmed.starts_with("[download]") && trimmed.contains('%') {
                if let Some(tx) = &progress_tx {
                    let _ = tx
                        .send(fsocial_common::ProgressEvent::Line(trimmed.to_string()))
                        .await;
                }
            }
        }
    }

    let timeout_duration = std::time::Duration::from_secs(3600); // 1 hour max
    let status = match tokio::time::timeout(timeout_duration, child.wait()).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(AppError::Download(format!("Ошибка процесса yt-dlp: {}", e))),
        Err(_) => {
            if let Some(pid) = _kill_guard.0 {
                #[cfg(unix)]
                unsafe {
                    libc::kill(-(pid as i32), libc::SIGKILL);
                }
                _kill_guard.0 = None;
            }
            let _ = child.kill().await;
            return Err(AppError::Download(
                "Процесс скачивания завис и был прерван (таймаут 1 час)".into(),
            ));
        }
    };
    _kill_guard.0 = None; // Disable kill on drop since it completed successfully
    if !status.success() {
        let stderr = error_log.lock().await.clone();
        error!("yt-dlp error: {}", stderr);

        if stderr.contains("No video formats found") && url.contains("pin") {
            let client = reqwest::Client::new();
            if let Ok(resp) = client.get(url).send().await {
                if let Ok(html) = resp.text().await {
                    let og_re = regex::Regex::new(r#"<meta(?:[^>]+)og:image(?:[^>]+)>"#).unwrap();
                    let content_re = regex::Regex::new(r#"content="([^"]+)""#).unwrap();

                    let mut img_url_opt = None;
                    if let Some(mat) = og_re.find(&html) {
                        let tag = mat.as_str();
                        if let Some(caps) = content_re.captures(tag) {
                            let url_str = caps.get(1).unwrap().as_str();
                            img_url_opt = Some(
                                url_str
                                    .replace("/736x/", "/originals/")
                                    .replace("/474x/", "/originals/")
                                    .replace("/236x/", "/originals/"),
                            );
                        }
                    } else {
                        // Fallback to JSON schema "image":"..."
                        let json_re =
                            regex::Regex::new(r#""image":"(https://i\.pinimg\.com/[^"]+)""#)
                                .unwrap();
                        if let Some(caps) = json_re.captures(&html) {
                            img_url_opt = Some(caps.get(1).unwrap().as_str().to_string());
                        }
                    }

                    if let Some(img_url) = img_url_opt {
                        let ext = img_url.split('.').last().unwrap_or("png");
                        let file_path = format!("{}/{}.{}", output_dir, uuid, ext);

                        if let Ok(img_resp) = client.get(&img_url).send().await {
                            if let Ok(bytes) = img_resp.bytes().await {
                                if tokio::fs::write(&file_path, &bytes).await.is_ok() {
                                    prefix_guard.cancel();
                                    return Ok(YtDlpOutput {
                                        file_path,
                                        title: "Pinterest Image".to_string(),
                                        duration: None,
                                        thumbnail: None,
                                        uploader: Some("Pinterest".to_string()),
                                    });
                                }
                            }
                        }
                    }
                }
            }
            return Err(AppError::Download(
                "Не удалось извлечь фото с Pinterest. Возможно ссылка битая.".into(),
            ));
        }

        return Err(AppError::YtDlp {
            message: stderr,
            exit_code: status.code().unwrap_or(-1),
        });
    }

    let info_path = format!("{}/{}.info.json", output_dir, uuid);
    let info_str = tokio::fs::read_to_string(&info_path)
        .await
        .unwrap_or_default();
    let json: Value = serde_json::from_str(&info_str).unwrap_or(serde_json::json!({}));
    let _ = tokio::fs::remove_file(&info_path).await;

    let mut file_path = String::new();
    let mut entries = match tokio::fs::read_dir(output_dir).await {
        Ok(e) => e,
        Err(e) => return Err(AppError::Download(format!("Не удалось прочитать папку загрузки: {}", e))),
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&uuid) && !name.ends_with(".json") {
            file_path = entry.path().to_string_lossy().to_string();
            break;
        }
    }

    if file_path.is_empty() {
        return Err(AppError::Download("Could not find downloaded file".into()));
    }

    let title = json
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("Unknown")
        .to_string();
    let duration = json
        .get("duration")
        .and_then(|v| v.as_f64())
        .map(|d| d as u64);
    let mut thumbnail = json
        .get("thumbnail")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    if thumbnail.is_none() {
        if let Some(thumbnails) = json.get("thumbnails").and_then(|v| v.as_array()) {
            if let Some(last) = thumbnails.last() {
                thumbnail = last
                    .get("url")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            }
        }
    }
    if let Some(mut t) = thumbnail.take() {
        if t.contains("ytimg.com") && t.contains("vi_webp") {
            t = t.replace("vi_webp", "vi").replace(".webp", ".jpg");
        }
        thumbnail = Some(t);
    }
    let uploader = json
        .get("uploader")
        .or_else(|| json.get("channel"))
        .or_else(|| json.get("uploader_id"))
        .or_else(|| json.get("artist"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    prefix_guard.cancel();
    Ok(YtDlpOutput {
        file_path,
        title,
        duration,
        thumbnail,
        uploader,
    })
}

#[instrument(skip(config, proxy))]
pub async fn get_info(
    config: &AppConfig,
    url: &str,
    proxy: Option<&str>,
) -> Result<fsocial_common::InfoResponse, AppError> {
    let mut cmd = Command::new(&config.ytdlp_path);
    cmd.arg("--dump-json").arg("--flat-playlist");

    if let Some(p) = proxy {
        cmd.arg("--proxy").arg(p);
    }

    if let Some(ref cookies) = config.cookies_path {
        cmd.arg("--cookies").arg(cookies);
    }

    cmd.arg(url);
    cmd.stdin(Stdio::null());

    let output = tokio::time::timeout(std::time::Duration::from_secs(25), cmd.output())
        .await
        .map_err(|_| AppError::YtDlp {
            message: "yt-dlp analysis timed out after 25 seconds".into(),
            exit_code: -1,
        })??;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);

        if stderr.contains("No video formats found") && url.contains("pin") {
            return Err(AppError::Download(
                "Скачивание фото с Pinterest пока не поддерживается, бот качает только видео!"
                    .into(),
            ));
        }

        return Err(AppError::YtDlp {
            message: stderr.into_owned(),
            exit_code: output.status.code().unwrap_or(-1),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    let mut is_playlist = false;
    let mut playlist_count = None;
    let mut playlist_urls = Vec::new();
    let mut title = String::new();
    let mut uploader = None;
    let mut thumbnail = None;
    let mut duration_secs = None;
    let mut raw_formats = None;
    let mut available_qualities = Vec::new();

    // If it's a playlist with flat-playlist, it will output one JSON per line.
    for line in stdout.lines() {
        if let Ok(json) = serde_json::from_str::<Value>(line) {
            if duration_secs.is_none() {
                duration_secs = json
                    .get("duration")
                    .and_then(|v| v.as_f64())
                    .map(|d| d as u64);
            }

            if uploader.is_none() {
                uploader = json
                    .get("uploader")
                    .or_else(|| json.get("channel"))
                    .or_else(|| json.get("uploader_id"))
                    .or_else(|| json.get("artist"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            }

            if thumbnail.is_none() {
                thumbnail = json
                    .get("thumbnail")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                if thumbnail.is_none() {
                    if let Some(thumbnails) = json.get("thumbnails").and_then(|v| v.as_array()) {
                        if let Some(last) = thumbnails.last() {
                            thumbnail = last
                                .get("url")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string());
                        }
                    }
                }
            }
            if let Some(mut t) = thumbnail.take() {
                if t.contains("ytimg.com") && t.contains("vi_webp") {
                    t = t.replace("vi_webp", "vi").replace(".webp", ".jpg");
                }
                thumbnail = Some(t);
            }

            if let Some(t) = json.get("_type").and_then(|v| v.as_str()) {
                if t == "playlist" || t == "multi_video" {
                    is_playlist = true;
                    if let Some(entries) = json.get("entries").and_then(|v| v.as_array()) {
                        playlist_count = Some(entries.len() as u32);
                        for entry in entries {
                            if let Some(entry_url) = entry.get("url").and_then(|v| v.as_str()) {
                                playlist_urls.push(entry_url.to_string());
                            }
                        }
                    }
                    if title.is_empty() {
                        title = json
                            .get("title")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Playlist")
                            .to_string();
                    }
                    continue;
                } else if t == "url" || t == "url_transparent" {
                    if let Some(entry_url) = json.get("url").and_then(|v| v.as_str()) {
                        playlist_urls.push(entry_url.to_string());
                    }
                    if title.is_empty() {
                        title = json
                            .get("playlist_title")
                            .or_else(|| json.get("playlist"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("Playlist")
                            .to_string();
                    }
                    if playlist_count.is_none() {
                        playlist_count = json
                            .get("playlist_count")
                            .and_then(|v| v.as_u64())
                            .map(|n| n as u32);
                    }
                    continue;
                }
            }

            // It's a single video (or first video of playlist)
            if title.is_empty() {
                title = json
                    .get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown")
                    .to_string();
            }

            if json
                .get("is_live")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                return Err(AppError::Download(
                    "Скачивание прямых трансляций (Live) не поддерживается".into(),
                ));
            }

            // Parse formats
            if let Some(formats) = json.get("formats").and_then(|v| v.as_array()) {
                raw_formats = Some(formats.clone());
                let mut has_audio = false;
                let mut heights = std::collections::HashSet::new();
                for f in formats {
                    let acodec = f.get("acodec").and_then(|v| v.as_str()).unwrap_or("none");
                    let vcodec = f.get("vcodec").and_then(|v| v.as_str()).unwrap_or("none");
                    if acodec != "none" {
                        has_audio = true;
                    }
                    if vcodec != "none" {
                        if let Some(height) = f.get("height").and_then(|v| v.as_i64()) {
                            heights.insert(height);
                        }
                    }
                }

                if has_audio {
                    available_qualities.push(Quality::AudioBest);
                }

                if !heights.is_empty() {
                    if heights.iter().any(|&h| h >= 240 && h < 400) {
                        available_qualities.push(Quality::Video360p);
                    }
                    if heights.iter().any(|&h| h >= 400 && h < 550) {
                        available_qualities.push(Quality::Video480p);
                    }
                    if heights.iter().any(|&h| h >= 700 && h < 850) {
                        available_qualities.push(Quality::Video720p);
                    }
                    if heights.iter().any(|&h| h >= 1000 && h < 1400) {
                        available_qualities.push(Quality::Video1080p);
                    }
                    if heights.iter().any(|&h| h >= 1400 && h < 2000) {
                        available_qualities.push(Quality::Video1440p);
                    }
                    if heights.iter().any(|&h| h >= 2000) {
                        available_qualities.push(Quality::Video4K);
                    }
                }
            }
        }
    }

    if available_qualities.is_empty() {
        available_qualities = Quality::audio_options();
    } else {
        available_qualities.dedup();
    }

    let mut sizes: Vec<(Quality, Option<u64>)> = available_qualities
        .into_iter()
        .map(|q| {
            let mut sz_bytes: Option<u64> = None;

            if let Some(ref fmts) = raw_formats {
                let target_height = match q {
                    Quality::Video360p => Some(360),
                    Quality::Video480p => Some(480),
                    Quality::Video720p => Some(720),
                    Quality::Video1080p => Some(1080),
                    Quality::Video1440p => Some(1440),
                    Quality::Video4K => Some(2160),
                    _ => None,
                };

                if let Some(th) = target_height {
                    let mut max_bytes = 0;
                    for f in fmts {
                        let vcodec = f.get("vcodec").and_then(|v| v.as_str()).unwrap_or("none");
                        if vcodec != "none" && !vcodec.contains("mhtml") {
                            if f.get("height").and_then(|v| v.as_i64()) == Some(th) {
                                if let Some(bytes) = f
                                    .get("filesize")
                                    .and_then(|v| v.as_u64())
                                    .or_else(|| f.get("filesize_approx").and_then(|v| v.as_u64()))
                                {
                                    max_bytes = max_bytes.max(bytes);
                                }
                            }
                        }
                    }
                    if max_bytes > 0 {
                        sz_bytes = Some(max_bytes);
                    }
                }
            }

            // Estimate from duration if not present
            if sz_bytes.is_none() {
                if let Some(d) = duration_secs {
                    let rate_mb_per_sec = match q {
                        Quality::Video4K => 4.0,
                        Quality::Video1440p => 2.5,
                        Quality::Video1080p => 1.5,
                        Quality::Video720p => 0.8,
                        Quality::Video480p => 0.4,
                        Quality::Video360p => 0.2,
                        Quality::Best => 2.0,
                        _ => 0.03, // audio
                    };
                    sz_bytes = Some((d as f64 * rate_mb_per_sec * 1024.0 * 1024.0) as u64);
                }
            }
            (q, sz_bytes)
        })
        .collect();

    // Enforce monotonic sizes for video
    let mut last_size = 0;
    for (q, sz_bytes) in &mut sizes {
        if !q.is_audio() {
            if let Some(size) = sz_bytes {
                if *size <= last_size && last_size > 0 {
                    // Make it 20% larger than the previous lower quality
                    *size = (last_size as f64 * 1.2) as u64;
                }
                last_size = *size;
            }
        }
    }

    let quality_options: Vec<fsocial_common::QualityOption> = sizes
        .into_iter()
        .map(|(q, sz_bytes)| {
            let mb = sz_bytes.map(|b| b / (1024 * 1024)).unwrap_or(0);
            let estimated_secs = sz_bytes.map(|b| (b / (10 * 1024 * 1024)).max(1)); // 10 MB/s speed

            let speed_category = match mb {
                0..=15 => "⚡ Ультра быстрая (~1-2 сек)".to_string(),
                16..=50 => "🚀 Быстрая (~3-5 сек)".to_string(),
                51..=150 => "⚖️ Нормальная (~5-15 сек)".to_string(),
                _ => "🐢 Медленная (>15 сек)".to_string(),
            };

            let display_label = if mb > 0 {
                format!("{} (~{} МБ)", q.display_name(), mb)
            } else {
                q.display_name().to_string()
            };

            let full_button_label = if mb > 0 {
                format!("{}  •  ~{} МБ", q.display_name(), mb)
            } else {
                q.display_name().to_string()
            };

            fsocial_common::QualityOption {
                quality: q,
                filesize_bytes: sz_bytes,
                estimated_secs,
                speed_category,
                display_label,
                full_button_label,
            }
        })
        .collect();

    Ok(fsocial_common::InfoResponse {
        title,
        uploader,
        thumbnail,
        duration_secs,
        available_qualities: quality_options,
        is_playlist: is_playlist || !playlist_urls.is_empty(),
        playlist_count: if !playlist_urls.is_empty() {
            Some(playlist_urls.len() as u32)
        } else {
            playlist_count
        },
        playlist_urls,
        error: None,
    })
}
