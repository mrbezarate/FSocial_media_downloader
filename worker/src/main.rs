use deadpool_redis::{Config as RedisConfig, Runtime};
use fsocial_common::AppConfig;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod audio;
mod info_handler;
mod media;
mod nats_consumer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let mut worker_type = "both".to_string();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--type" {
            if let Some(t) = args.next() {
                worker_type = t;
            }
        }
    }

    info!("Starting fsocial-worker (type: {})...", worker_type);

    let config = AppConfig::from_env()?;

    let nats_client = async_nats::connect(&config.nats_url).await?;
    let nats_jetstream = async_nats::jetstream::new(nats_client.clone());

    let redis_cfg = RedisConfig::from_url(&config.redis_url);
    let redis_pool = redis_cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

    let proxy_pool = media::proxy::ProxyPool::new(config.proxy_list.clone());
    let cache = media::cache::MetadataCache::new(redis_pool.clone());

    let worker_ctx = Arc::new(nats_consumer::WorkerContext {
        config,
        nats_client,
        nats_jetstream,
        redis_pool,
        cache,
        proxy_pool,
        task_states: moka::future::Cache::builder()
            .time_to_live(std::time::Duration::from_secs(4 * 3600))
            .build(),
    });

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let ctx_clone = worker_ctx.clone();
    let worker_handle = tokio::spawn(async move {
        nats_consumer::run(ctx_clone, shutdown_rx, worker_type).await;
    });

    tokio::signal::ctrl_c().await?;
    info!(
        "Shutting down fsocial-worker gracefully. Waiting for ongoing downloads (timeout 120s)..."
    );
    let _ = shutdown_tx.send(true);

    match tokio::time::timeout(tokio::time::Duration::from_secs(120), worker_handle).await {
        Ok(_) => info!("Graceful shutdown complete."),
        Err(_) => tracing::error!("Shutdown timed out. Forcing exit."),
    }

    Ok(())
}
