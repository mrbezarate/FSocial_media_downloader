use fsocial_common::{AppConfig, DownloadTask, Quality};
use teloxide::prelude::*;
use tracing::error;

use crate::{nats_client::NatsClient, url_parser, UrlCache};

pub async fn handle(
    bot: Bot,
    q: CallbackQuery,
    nats: NatsClient,
    _config: AppConfig,
    url_cache: UrlCache,
) -> ResponseResult<()> {
    if let Some(data) = &q.data {
        let parts: Vec<&str> = data.splitn(2, '|').collect();
        if parts.len() == 2 {
            let quality_str = parts[0];
            let short_id = parts[1];
            
            // Retrieve actual URL from cache
            let original_url = url_cache.lock().await.remove(short_id);

            if let Some(quality) = Quality::from_callback(quality_str) {
                if let Some(url_str) = original_url {
                    if let Some(url_match) = url_parser::detect(&url_str) {
                    bot.answer_callback_query(q.id.clone()).await?;

                    if let Some(msg) = q.message {
                        if let Err(_) = bot
                            .edit_message_text(msg.chat().id, msg.id(), "⏳ Загружаю...")
                            .reply_markup(teloxide::types::InlineKeyboardMarkup::default())
                            .await
                        {
                            let _ = bot
                                .edit_message_caption(msg.chat().id, msg.id())
                                .caption("⏳ Загружаю...")
                                .reply_markup(teloxide::types::InlineKeyboardMarkup::default())
                                .await;
                        }

                        let user_id = q.from.id.0;
                        let chat_id = msg.chat().id.0;
                        let message_id = msg.id().0;
                        let chat = msg.chat().id;

                        // Check if it's a playlist
                        let req = fsocial_common::InfoRequest { url: url_match.url.clone() };
                        if let Ok(info) = nats.request_info(&req).await {
                            if info.is_playlist && !info.playlist_urls.is_empty() {
                                let playlist_status = format!("⏳ Скачивание плейлиста: 0/{}\nПрогресс: 0%", info.playlist_urls.len());
                                if let Err(_) = bot
                                    .edit_message_text(chat, msg.id(), playlist_status.clone())
                                    .reply_markup(teloxide::types::InlineKeyboardMarkup::default())
                                    .await
                                {
                                    let _ = bot
                                        .edit_message_caption(chat, msg.id())
                                        .caption(playlist_status)
                                        .reply_markup(teloxide::types::InlineKeyboardMarkup::default())
                                        .await;
                                }

                                let mut task = DownloadTask::new(
                                    url_match.url.clone(),
                                    url_match.platform.clone(),
                                    url_match.media_type.clone(),
                                    quality.clone(),
                                    chat_id,
                                    message_id,
                                    user_id,
                                    false,
                                );
                                task.status_message_id = Some(msg.id().0);
                                task.playlist_urls = Some(info.playlist_urls.clone());
                                let _ = nats.publish_task(&task).await;
                                return Ok(());
                            }
                        }

                        let mut task = DownloadTask::new(
                            url_match.url,
                            url_match.platform,
                            url_match.media_type,
                            quality,
                            chat_id,
                            message_id,
                            user_id,
                            false,
                        );
                        task.status_message_id = Some(message_id);

                        if let Err(e) = nats.publish_task(&task).await {
                            error!("Failed to publish task: {}", e);
                            if let Err(_) = bot.edit_message_text(msg.chat().id, msg.id(), "❌ Внутренняя ошибка").await {
                                let _ = bot.edit_message_caption(msg.chat().id, msg.id()).caption("❌ Внутренняя ошибка").await;
                            }
                        }
                    }
                    return Ok(());
                }
                }
            }
        }
    }

    bot.answer_callback_query(q.id)
        .text("❌ Ошибка обработки запроса")
        .await?;

    Ok(())
}
