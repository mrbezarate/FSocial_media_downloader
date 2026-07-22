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
) -> Result<(String, String, Option<u64>), AppError> {
    if let Some(cached) = ctx.cache.get(&task.url).await {
        info!("Cache hit for URL: {}", task.url);
        return Ok((cached.file_path, cached.title, cached.duration));
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
        ).await {
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
                
                return Ok((output.file_path, output.title, output.duration));
            }
            Err(e) => {
                if let Some(p) = proxy {
                    ctx.proxy_pool.mark_failed(p);
                }
                
                if attempts >= max_attempts {
                    warn!("Failed to download media after {} attempts. Last error: {:?}", attempts, e);
                    return Err(e);
                }
            }
        }
    }
}
