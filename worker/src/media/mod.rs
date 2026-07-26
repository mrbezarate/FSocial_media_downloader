pub mod cache;
pub mod proxy;
pub mod ytdlp;

use crate::nats_consumer::WorkerContext;
use fsocial_common::{AppError, DownloadTask};
use tracing::{info, warn};

pub async fn process_media_task(
    ctx: &WorkerContext,
    task: &DownloadTask,
    progress_tx: Option<tokio::sync::mpsc::Sender<fsocial_common::ProgressEvent>>,
) -> Result<(String, String, Option<u64>, Option<String>, Option<String>), AppError> {
    if let Some(cached) = ctx.cache.get(&task.url).await {
        info!("Cache hit for URL: {}", task.url);
        // In a real app we'd also cache the thumbnail path, but for now just return None for cached items
        return Ok((cached.file_path, cached.title, cached.duration, None, None));
    }

    let mut attempts = 0;
    let max_attempts = if ctx.config.proxies_enabled() { 3 } else { 1 };

    loop {
        attempts += 1;
        let proxy = ctx.proxy_pool.next();
        let tx_clone = progress_tx.clone();

        match ytdlp::download(
            &ctx.config,
            &task.url,
            &task.quality,
            &ctx.config.shared_data_path,
            proxy,
            tx_clone,
            task.is_premium,
        )
        .await
        {
            Ok(output) => {
                if let Some(p) = proxy {
                    ctx.proxy_pool.mark_success(p);
                }

                let cached = cache::CachedMedia {
                    file_path: output.file_path.clone(),
                    title: output.title.clone(),
                    duration: output.duration,
                };
                let _ = ctx.cache.set(&task.url, cached).await;

                let mut thumb_path = None;
                if let Some(thumb_url) = output.thumbnail {
                    match crate::audio::tagger::download_cover(&thumb_url).await {
                        Ok(cover_data) => {
                            let cover_path = format!("{}_cover.jpg", output.file_path);
                            if let Ok(_) = tokio::fs::write(&cover_path, &cover_data).await {
                                let _ = std::process::Command::new("ffmpeg")
                                    .args(&[
                                        "-y",
                                        "-i",
                                        &cover_path,
                                        "-vf",
                                        "scale=320:320:force_original_aspect_ratio=decrease",
                                        &format!("{}_tmp.jpg", cover_path),
                                    ])
                                    .output();
                                let _ =
                                    std::fs::rename(format!("{}_tmp.jpg", cover_path), &cover_path);
                                thumb_path = Some(cover_path);

                                // Optionally apply the cover to the MP3 metadata if it's audio
                                if task.quality.is_audio() {
                                    // Use a dummy SpotifyTrackMeta to just set the cover and author
                                    let dummy_meta = fsocial_common::SpotifyTrackMeta {
                                        title: output.title.clone(),
                                        artists: output
                                            .uploader
                                            .clone()
                                            .map(|s| vec![s])
                                            .unwrap_or_default(),
                                        album: "Single".to_string(),
                                        year: None,
                                        track_number: None,
                                        total_tracks: None,
                                        isrc: None,
                                        cover_url: Some(thumb_url),
                                        duration_ms: output.duration.unwrap_or(0) * 1000,
                                        genres: vec![],
                                    };
                                    let _ = crate::audio::tagger::apply_tags(
                                        &output.file_path,
                                        &dummy_meta,
                                        Some(cover_data),
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Failed to download cover for media: {}", e);
                        }
                    }
                } else if task.quality.is_audio() && output.uploader.is_some() {
                    let dummy_meta = fsocial_common::SpotifyTrackMeta {
                        title: output.title.clone(),
                        artists: vec![output.uploader.clone().unwrap()],
                        album: "Single".to_string(),
                        year: None,
                        track_number: None,
                        total_tracks: None,
                        isrc: None,
                        cover_url: None,
                        duration_ms: output.duration.unwrap_or(0) * 1000,
                        genres: vec![],
                    };
                    let _ = crate::audio::tagger::apply_tags(&output.file_path, &dummy_meta, None)
                        .await;
                }

                return Ok((
                    output.file_path,
                    output.title,
                    output.duration,
                    output.uploader,
                    thumb_path,
                ));
            }
            Err(e) => {
                if let Some(p) = proxy {
                    ctx.proxy_pool.mark_failed(p);
                }

                if attempts >= max_attempts {
                    warn!(
                        "Failed to download media after {} attempts. Last error: {:?}",
                        attempts, e
                    );
                    return Err(e);
                }
            }
        }
    }
}
