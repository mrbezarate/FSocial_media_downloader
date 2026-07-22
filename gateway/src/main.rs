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
    
    let bot = if let Some(ref url) = config.telegram_api_url {
        let api_url = url::Url::parse(url).expect("Invalid TELEGRAM_API_URL");
        Bot::with_client(token, client).set_api_url(api_url)
    } else {
        Bot::with_client(token, client)
    };

    let bot_clone = bot.clone();
    let nats_clone = nats_client.clone();
    let config_clone = config.clone();
    
    tokio::spawn(async move {
        nats_listener::listen(bot_clone, nats_clone, config_clone).await;
    });

    info!("NATS listener started. Setting up Telegram handlers...");

    let url_cache: UrlCache = Arc::new(Mutex::new(HashMap::new()));

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
        .dependencies(dptree::deps![nats_client.clone(), config.clone(), url_cache.clone()])
        .enable_ctrlc_handler()
        .build();

    info!("Gateway is running!");
    dispatcher.dispatch().await;

    Ok(())
}
