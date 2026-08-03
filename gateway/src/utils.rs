use teloxide::prelude::*;
use teloxide::types::{MessageId, ChatId};
use teloxide::requests::Requester;

pub const BOT_WATERMARK: &str = "\n\nСкачано с помощью бота @FSocial_Media_Downloader_bot";

pub async fn edit_message_text_or_caption(
    bot: &crate::MyBot,
    chat_id: ChatId,
    msg_id: MessageId,
    text: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Err(_) = bot.edit_message_text(chat_id, msg_id, text.clone()).await {
        bot.edit_message_caption(chat_id, msg_id).caption(text).await?;
    }
    Ok(())
}

pub async fn get_cached_file_id(redis_pool: &deadpool_redis::Pool, quality_id: &str, url: &str) -> Option<String> {
    let cache_key = format!("file_id:{}:{}", quality_id, url);
    if let Ok(mut conn) = redis_pool.get().await {
        let res: redis::RedisResult<String> = redis::cmd("GET")
            .arg(&cache_key)
            .query_async(&mut conn)
            .await;
        return res.ok();
    }
    None
}
