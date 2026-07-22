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
}

use tokio::io::{AsyncBufReadExt, BufReader};

#[instrument(skip(config, progress_tx))]
pub async fn download(
    config: &AppConfig,
    url: &str,
    quality: &Quality,
    output_dir: &str,
    proxy: Option<&str>,
    progress_tx: Option<tokio::sync::mpsc::Sender<String>>,
) -> Result<YtDlpOutput, AppError> {
    let uuid = uuid::Uuid::new_v4().to_string();
    let mut cmd = Command::new(&config.ytdlp_path);
    
    cmd.arg("--no-warnings")
       .arg("-f").arg(quality.ytdlp_format())
       .arg("-o").arg(format!("{}/{}.%(ext)s", output_dir, uuid))
       .arg("--newline")
       .arg("--write-info-json")
       .arg("--no-playlist")
       .arg("-N").arg("4") // 4 concurrent fragments to speed up downloads
       .arg("--ffmpeg-location").arg(&config.ffmpeg_path);

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

    cmd.arg(url);

    info!("Running yt-dlp command: {:?}", cmd);
    let mut child = cmd.stdout(Stdio::piped())
                       .stderr(Stdio::piped())
                       .spawn()?;

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
        if line.starts_with("[download]") && line.contains("%") {
            if let Some(tx) = &progress_tx {
                let _ = tx.send(line).await;
            }
        }
    }

    let status = child.wait().await?;
    if !status.success() {
        let stderr = error_log.lock().await.clone();
        error!("yt-dlp error: {}", stderr);
        return Err(AppError::YtDlp {
            message: stderr,
            exit_code: status.code().unwrap_or(-1),
        });
    }

    let info_path = format!("{}/{}.info.json", output_dir, uuid);
    let info_str = tokio::fs::read_to_string(&info_path).await.unwrap_or_default();
    let json: Value = serde_json::from_str(&info_str).unwrap_or(serde_json::json!({}));
    let _ = tokio::fs::remove_file(&info_path).await;

    let mut file_path = String::new();
    let mut entries = tokio::fs::read_dir(output_dir).await.unwrap();
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

    let title = json.get("title").and_then(|v| v.as_str()).unwrap_or("Unknown").to_string();
    let duration = json.get("duration").and_then(|v| v.as_f64()).map(|d| d as u64);
    let thumbnail = json.get("thumbnail").and_then(|v| v.as_str()).map(|s| s.to_string());

    Ok(YtDlpOutput {
        file_path,
        title,
        duration,
        thumbnail,
    })
}

#[instrument(skip(config, proxy))]
pub async fn get_info(config: &AppConfig, url: &str, proxy: Option<&str>) -> Result<fsocial_common::InfoResponse, AppError> {
    let mut cmd = Command::new(&config.ytdlp_path);
    cmd.arg("--dump-json")
       .arg("--flat-playlist");
       
    if let Some(p) = proxy {
        cmd.arg("--proxy").arg(p);
    }
    
    if let Some(ref cookies) = config.cookies_path {
        cmd.arg("--cookies").arg(cookies);
    }
    
    cmd.arg(url);
    
    let output = tokio::time::timeout(std::time::Duration::from_secs(25), cmd.output())
        .await
        .map_err(|_| AppError::YtDlp {
            message: "yt-dlp analysis timed out after 25 seconds".into(),
            exit_code: -1,
        })??;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
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
                duration_secs = json.get("duration").and_then(|v| v.as_f64()).map(|d| d as u64);
            }

            if uploader.is_none() {
                uploader = json.get("uploader")
                    .or_else(|| json.get("channel"))
                    .or_else(|| json.get("uploader_id"))
                    .or_else(|| json.get("artist"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            }

            if thumbnail.is_none() {
                thumbnail = json.get("thumbnail").and_then(|v| v.as_str()).map(|s| s.to_string());
                if thumbnail.is_none() {
                    if let Some(thumbnails) = json.get("thumbnails").and_then(|v| v.as_array()) {
                        if let Some(last) = thumbnails.last() {
                            thumbnail = last.get("url").and_then(|v| v.as_str()).map(|s| s.to_string());
                        }
                    }
                }
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
                        title = json.get("title").and_then(|v| v.as_str()).unwrap_or("Playlist").to_string();
                    }
                    continue;
                } else if t == "url" {
                    if let Some(entry_url) = json.get("url").and_then(|v| v.as_str()) {
                        playlist_urls.push(entry_url.to_string());
                    }
                    continue;
                }
            }

            // It's a single video (or first video of playlist)
            if title.is_empty() {
                title = json.get("title").and_then(|v| v.as_str()).unwrap_or("Unknown").to_string();
            }

            // Parse formats
            if let Some(formats) = json.get("formats").and_then(|v| v.as_array()) {
                raw_formats = Some(formats.clone());
                let mut has_audio = false;
                let mut max_height = 0;
                for f in formats {
                    let acodec = f.get("acodec").and_then(|v| v.as_str()).unwrap_or("none");
                    let vcodec = f.get("vcodec").and_then(|v| v.as_str()).unwrap_or("none");
                    if acodec != "none" {
                        has_audio = true;
                    }
                    if vcodec != "none" {
                        if let Some(height) = f.get("height").and_then(|v| v.as_i64()) {
                            max_height = max_height.max(height);
                        }
                    }
                }

                if has_audio {
                    available_qualities.push(Quality::AudioBest);
                }
                
                if max_height > 0 {
                    if max_height >= 360 { available_qualities.push(Quality::Video360p); }
                    if max_height >= 480 { available_qualities.push(Quality::Video480p); }
                    if max_height >= 720 { available_qualities.push(Quality::Video720p); }
                    if max_height >= 1080 { available_qualities.push(Quality::Video1080p); }
                    if max_height >= 2160 { available_qualities.push(Quality::Video4K); }
                }
            }
        }
    }

    if available_qualities.is_empty() {
        available_qualities = Quality::audio_options();
    } else {
        available_qualities.dedup();
    }

    let quality_options: Vec<fsocial_common::QualityOption> = available_qualities
        .into_iter()
        .map(|q| {
            let mut sz_bytes: Option<u64> = None;

            if let Some(ref fmts) = raw_formats {
                let target_height = match q {
                    Quality::Video360p => Some(360),
                    Quality::Video480p => Some(480),
                    Quality::Video720p => Some(720),
                    Quality::Video1080p => Some(1080),
                    Quality::Video4K => Some(2160),
                    _ => None,
                };

                if let Some(th) = target_height {
                    for f in fmts {
                        if f.get("height").and_then(|v| v.as_i64()) == Some(th) {
                            if let Some(bytes) = f.get("filesize").and_then(|v| v.as_u64()).or_else(|| f.get("filesize_approx").and_then(|v| v.as_u64())) {
                                sz_bytes = Some(bytes);
                                break;
                            }
                        }
                    }
                }
            }

            // Estimate from duration if not present
            if sz_bytes.is_none() {
                if let Some(d) = duration_secs {
                    let rate_mb_per_sec = match q {
                        Quality::Video4K => 4.0,
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
        playlist_count: if !playlist_urls.is_empty() { Some(playlist_urls.len() as u32) } else { playlist_count },
        playlist_urls,
        error: None,
    })
}
