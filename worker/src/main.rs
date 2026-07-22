use fsocial_common::AppConfig;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;
use deadpool_redis::{Config as RedisConfig, Runtime};

mod audio;
mod media;
mod nats_consumer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    info!("Starting fsocial-worker...");

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
    });

    let ctx_clone = worker_ctx.clone();
    tokio::spawn(async move {
        nats_consumer::run(ctx_clone).await;
    });

    tokio::signal::ctrl_c().await?;
    info!("Shutting down fsocial-worker gracefully.");

    Ok(())
}
