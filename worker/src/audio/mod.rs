pub mod matcher;
pub mod spotify;
pub mod tagger;

use crate::nats_consumer::WorkerContext;
use crate::media::ytdlp;
use fsocial_common::{AppError, DownloadTask, Quality};
use tracing::info;

pub async fn process_spotify_task(
    ctx: &WorkerContext,
    task: &DownloadTask,
    progress_tx: Option<tokio::sync::mpsc::Sender<String>>,
) -> Result<(String, String, Option<u64>, Option<String>), AppError> {
    let spotify_client = spotify::SpotifyClient::new();
    
    let meta = if let Some(m) = &task.spotify_meta {
        m.clone()
    } else {
        if let Some((t, id)) = spotify::SpotifyClient::parse_spotify_url(&task.url) {
            if t == spotify::SpotifyType::Track {
                spotify_client.get_track(&ctx.config, &id).await?
            } else {
                return Err(AppError::Spotify("Only single tracks supported via direct task".into()));
            }
        } else {
            return Err(AppError::Spotify("Invalid spotify URL".into()));
        }
    };

    let yt_url = matcher::find_on_youtube(&ctx.config, &meta).await?;
    info!("Matched Spotify track to YouTube URL: {}", yt_url);

    let proxy = ctx.proxy_pool.next();
    let ytdlp_out = ytdlp::download(
        &ctx.config,
        &yt_url,
        &Quality::AudioBest,
        &ctx.config.shared_data_path,
        proxy,
        progress_tx,
    ).await?;

    if let Some(p) = proxy {
        ctx.proxy_pool.mark_success(p);
    }

    let mut cover_data = None;
    if let Some(ref cover_url) = meta.cover_url {
        match tagger::download_cover(cover_url).await {
            Ok(c) => cover_data = Some(c),
            Err(e) => info!("Failed to download cover: {:?}", e),
        }
    }

    tagger::apply_tags(&ytdlp_out.file_path, &meta, cover_data).await?;

    Ok((
        ytdlp_out.file_path,
        meta.title.clone(),
        Some(meta.duration_ms / 1000),
        Some(meta.primary_artist().to_string())
    ))
}
