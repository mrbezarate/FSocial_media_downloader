pub mod matcher;
pub mod spotify;
pub mod tagger;

use crate::media::ytdlp;
use crate::nats_consumer::WorkerContext;
use fsocial_common::{AppError, DownloadTask, Quality};
use tracing::info;

pub async fn process_spotify_task(
    ctx: &WorkerContext,
    task: &DownloadTask,
    progress_tx: Option<tokio::sync::mpsc::Sender<fsocial_common::ProgressEvent>>,
) -> Result<(String, String, Option<u64>, Option<String>, Option<String>), AppError> {
    let spotify_client = spotify::SpotifyClient::new();

    let meta = if let Some(m) = &task.spotify_meta {
        m.clone()
    } else {
        if let Some((t, id)) = spotify::SpotifyClient::parse_spotify_url(&task.url) {
            if t == spotify::SpotifyType::Track {
                spotify_client.get_track(&ctx.config, &id).await?
            } else {
                return Err(AppError::Spotify(
                    "Only single tracks supported via direct task".into(),
                ));
            }
        } else {
            return Err(AppError::Spotify("Invalid spotify URL".into()));
        }
    };

    let mut attempts = 0;
    let max_attempts = if ctx.config.proxies_enabled() { 3 } else { 1 };

    let ytdlp_out = loop {
        attempts += 1;
        let proxy = ctx.proxy_pool.next();

        match matcher::find_track_url(&ctx.config, &meta, proxy).await {
            Ok(yt_url) => {
                info!("Matched Spotify track to platform URL: {}", yt_url);

                match ytdlp::download(
                    &ctx.config,
                    &yt_url,
                    &Quality::AudioBest,
                    &ctx.config.shared_data_path,
                    proxy,
                    progress_tx.clone(),
                    task.is_premium,
                )
                .await
                {
                    Ok(out) => {
                        if let Some(p) = proxy {
                            ctx.proxy_pool.mark_success(p);
                        }
                        break out;
                    }
                    Err(e) => {
                        if let Some(p) = proxy {
                            ctx.proxy_pool.mark_failed(p);
                        }
                        if attempts >= max_attempts {
                            return Err(e);
                        }
                    }
                }
            }
            Err(e) => {
                if let Some(p) = proxy {
                    ctx.proxy_pool.mark_failed(p);
                }
                if attempts >= max_attempts {
                    return Err(e);
                }
            }
        }
    };

    let mut cover_data = None;
    let mut thumb_path = None;

    if let Some(ref cover_url) = meta.cover_url {
        match tagger::download_cover(cover_url).await {
            Ok(c) => {
                cover_data = Some(c.clone());
                // Save cover to disk for Telegram API
                let cover_path = format!("{}_cover.jpg", ytdlp_out.file_path);
                if let Ok(_) = tokio::fs::write(&cover_path, &c).await {
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
                    let _ = std::fs::rename(format!("{}_tmp.jpg", cover_path), &cover_path);
                    thumb_path = Some(cover_path);
                }
            }
            Err(e) => info!("Failed to download cover: {:?}", e),
        }
    }

    tagger::apply_tags(&ytdlp_out.file_path, &meta, cover_data).await?;

    Ok((
        ytdlp_out.file_path,
        meta.title.clone(),
        Some(meta.duration_ms / 1000),
        Some(meta.primary_artist().to_string()),
        thumb_path,
    ))
}
