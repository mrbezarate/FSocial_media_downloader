use dotenvy::dotenv;
use fsocial_common::*;
use teloxide::prelude::*;
use tracing::info;
use tracing_subscriber::EnvFilter;

use tokio::sync::Mutex;

mod admin_logs;
mod commands;
mod handlers;
mod nats_client;
mod nats_listener;
mod ui;
mod url_parser;
pub mod utils;

pub type UrlCache = moka::future::Cache<String, String>;
pub type TaskStates = moka::future::Cache<String, String>;

pub type MyBot = teloxide::adaptors::CacheMe<teloxide::adaptors::Throttle<teloxide::Bot>>;

use nats_client::NatsClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenv();
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let env_filter = EnvFilter::from_default_env();
    let fmt_layer = tracing_subscriber::fmt::layer();
    let memory_layer = admin_logs::MemoryLogLayer;

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(memory_layer)
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

    let url_cache: UrlCache = moka::future::Cache::builder()
        .time_to_live(std::time::Duration::from_secs(4 * 3600))
        .build();
    let task_states: TaskStates = moka::future::Cache::builder()
        .time_to_live(std::time::Duration::from_secs(4 * 3600))
        .build();

    let ts_clone = task_states.clone();

    let redis_cfg = deadpool_redis::Config::from_url(&config.redis_url);
    let redis_pool = redis_cfg
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .expect("Failed to create Redis pool");
    let redis_pool_clone = redis_pool.clone();

    tokio::spawn(async move {
        nats_listener::listen(
            bot_clone,
            nats_clone,
            config_clone,
            ts_clone,
            redis_pool_clone,
        )
        .await;
    });

    info!("NATS listener started. Setting up Telegram handlers...");

    let handler = Update::filter_message()
        .filter(|msg: Message| {
            tracing::info!("Received message: {:?}", msg.text());
            true
        })
        .branch(
            dptree::entry()
                .filter_command::<commands::Command>()
                .endpoint(commands::handle),
        )
        .branch(
            dptree::filter(|msg: Message| msg.successful_payment().is_some())
                .endpoint(handlers::payments::handle_successful_payment),
        )
        // Add state filter here: only fall through if state handle returns Ok(()) (meaning no state or state ignored)
        .branch(dptree::filter_async(|msg: Message, pool: deadpool_redis::Pool| async move {
            let uid = msg.from.as_ref().map(|u| u.id.0).unwrap_or(0);
            if uid == 0 { return false; }
            if let Ok(mut conn) = pool.get().await {
                let key = format!("admin_state:{}", uid);
                let state: Option<String> = redis::cmd("GET").arg(&key).query_async(&mut conn).await.unwrap_or(None);
                return state.is_some() && !state.as_ref().unwrap().is_empty();
            }
            false
        }).endpoint(handlers::state::handle))
        .branch(dptree::filter(url_parser::contains_url).endpoint(handlers::download::handle));

    let callback_handler = Update::filter_callback_query().endpoint(handlers::callback::handle);
    let inline_handler = Update::filter_inline_query().endpoint(handlers::inline::handle);
    let pre_checkout_handler =
        Update::filter_pre_checkout_query().endpoint(handlers::payments::handle_pre_checkout_query);

    let mut dispatcher = Dispatcher::builder(
        bot.clone(),
        dptree::entry()
            .branch(handler)
            .branch(callback_handler)
            .branch(inline_handler)
            .branch(pre_checkout_handler),
    )
    .dependencies(dptree::deps![
        nats_client.clone(),
        config.clone(),
        url_cache.clone(),
        task_states.clone(),
        redis_pool.clone()
    ])
    .enable_ctrlc_handler()
    .build();

    let commands = vec![
        teloxide::types::BotCommand::new("start", "🚀 Запустить бота / Помощь"),
        teloxide::types::BotCommand::new("settings", "⚙️ Настройки качества и звука"),
        teloxide::types::BotCommand::new("promo", "🎟 Ввести промокод"),
        teloxide::types::BotCommand::new("help", "❓ Как пользоваться ботом"),
    ];

    // Explicitly delete commands first to clear Telegram cache of /admin
    let _ = bot.delete_my_commands().await;

    if let Err(e) = bot.set_my_commands(commands.clone()).await {
        tracing::warn!("Failed to set bot commands: {}", e);
    }

    info!("Gateway is running!");
    dispatcher.dispatch().await;

    Ok(())
}
