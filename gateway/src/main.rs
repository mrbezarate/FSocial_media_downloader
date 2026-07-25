use dotenvy::dotenv;
use fsocial_common::*;
use teloxide::prelude::*;
use tracing::info;
use tracing_subscriber::EnvFilter;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

mod commands;
mod handlers;
mod nats_client;
mod nats_listener;
mod ui;
mod url_parser;

pub type UrlCache = Arc<Mutex<HashMap<String, String>>>;
pub type TaskStates = moka::future::Cache<String, String>;

pub type MyBot = teloxide::adaptors::CacheMe<teloxide::adaptors::Throttle<teloxide::Bot>>;

use nats_client::NatsClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    info!("Starting Gateway service...");

    let config = AppConfig::from_env().expect("Failed to load config");

    let nats_client = NatsClient::connect(&config.nats_url)
        .await
        .expect("Failed to connect to NATS");
    
    nats_client
        .setup_stream()
        .await
        .expect("Failed to setup NATS stream");

    info!("Connected to NATS");

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(600)) // 10 minutes timeout for large video uploads
        .build()
        .expect("Failed to create reqwest client");

    let token = std::env::var("TELOXIDE_TOKEN").expect("TELOXIDE_TOKEN must be set");
    
    let base_bot = if let Some(ref url) = config.telegram_api_url {
        let api_url = url::Url::parse(url).expect("Invalid TELEGRAM_API_URL");
        Bot::with_client(token, client).set_api_url(api_url)
    } else {
        Bot::with_client(token, client)
    };

    use teloxide::adaptors::throttle::Limits;
    let bot = base_bot.throttle(Limits::default()).cache_me();

    let bot_clone = bot.clone();
    let nats_clone = nats_client.clone();
    let config_clone = config.clone();
    
    let url_cache: UrlCache = Arc::new(Mutex::new(HashMap::new()));
    let task_states: TaskStates = moka::future::Cache::builder()
        .time_to_live(std::time::Duration::from_secs(4 * 3600))
        .build();

    let ts_clone = task_states.clone();
    
    let redis_cfg = deadpool_redis::Config::from_url(&config.redis_url);
    let redis_pool = redis_cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1)).expect("Failed to create Redis pool");
    let redis_pool_clone = redis_pool.clone();

    tokio::spawn(async move {
        nats_listener::listen(bot_clone, nats_clone, config_clone, ts_clone, redis_pool_clone).await;
    });

    info!("NATS listener started. Setting up Telegram handlers...");

    let handler = Update::filter_message()
        .branch(
            dptree::entry()
                .filter_command::<commands::Command>()
                .endpoint(commands::handle),
        )
        .branch(
            dptree::filter(url_parser::contains_url).endpoint(handlers::download::handle),
        );

    let callback_handler = Update::filter_callback_query().endpoint(handlers::callback::handle);

    let mut dispatcher = Dispatcher::builder(bot.clone(), dptree::entry().branch(handler).branch(callback_handler))
        .dependencies(dptree::deps![nats_client.clone(), config.clone(), url_cache.clone(), task_states.clone(), redis_pool.clone()])
        .enable_ctrlc_handler()
        .build();

    info!("Gateway is running!");
    dispatcher.dispatch().await;

    Ok(())
}
