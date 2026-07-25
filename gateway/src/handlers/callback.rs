use fsocial_common::{AppConfig, DownloadTask, Quality};
use teloxide::prelude::*;
use tracing::error;

use crate::{nats_client::NatsClient, url_parser, UrlCache};

pub async fn handle(
    bot: crate::MyBot,
    q: CallbackQuery,
    nats: NatsClient,
    _config: AppConfig,
    url_cache: UrlCache,
    task_states: crate::TaskStates,
    redis_pool: deadpool_redis::Pool,
) -> ResponseResult<()> {
    if let Some(data) = &q.data {
        let parts: Vec<&str> = data.splitn(2, '|').collect();
        
        if parts[0] == "buy_premium" {
            let title = "Premium Подписка 💎";
            let description = "Месяц безграничных загрузок: неограниченный трафик, плейлисты любой длины и максимальный приоритет в очереди!";
            let payload = "premium_1_month";
            let provider_token = ""; // Оставляем пустым для Telegram Stars
            let currency = "XTR"; // Telegram Stars
            let prices = vec![teloxide::types::LabeledPrice {
                label: "1 Месяц Premium".into(),
                amount: 500, // 500 звезд
            }];

            if let Some(msg) = q.message.as_ref() {
                let _ = bot.send_invoice(msg.chat().id, title, description, payload, currency, prices).await;
            }
            let _ = bot.answer_callback_query(q.id).await;
            return Ok(());
        }

        if parts[0].starts_with("set_") || parts[0].starts_with("setmenu") || parts[0].starts_with("settings") || parts[0] == "toggle_mode" {
            let action = parts[0];
            let target = if parts.len() > 1 { parts[1] } else { "" };
            let user_id = q.from.id.0;
            
            let mut settings = fsocial_common::UserSettings::default();
            if let Ok(mut conn) = redis_pool.get().await {
                let key = format!("user_settings:{}", user_id);
                let res: redis::RedisResult<String> = redis::cmd("GET").arg(&key).query_async(&mut conn).await;
                if let Ok(val) = res {
                    if let Ok(parsed) = serde_json::from_str::<fsocial_common::UserSettings>(&val) {
                        settings = parsed;
                    }
                }
                
                let mut should_save = false;
                
                if action == "toggle_mode" {
                    settings.auto_download = !settings.auto_download;
                    should_save = true;
                } else if action == "setmenu" {
                    if let Some(msg) = q.message.as_ref() {
                        use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};
                        let mut btns = Vec::new();
                        if target == "vid" {
                            for chunk in fsocial_common::Quality::video_options().chunks(2) {
                                let mut row = Vec::new();
                                for q_opt in chunk {
                                    row.push(InlineKeyboardButton::callback(format!("{:?}", q_opt), format!("set_vid|{}", q_opt.callback_id())));
                                }
                                btns.push(row);
                            }
                        } else if target == "aud" {
                            for chunk in fsocial_common::Quality::audio_options().chunks(2) {
                                let mut row = Vec::new();
                                for q_opt in chunk {
                                    row.push(InlineKeyboardButton::callback(format!("{:?}", q_opt), format!("set_aud|{}", q_opt.callback_id())));
                                }
                                btns.push(row);
                            }
                        }
                        btns.push(vec![InlineKeyboardButton::callback("[ Назад ]", "settings_main")]);
                        let _ = bot.edit_message_reply_markup(msg.chat().id, msg.id()).reply_markup(InlineKeyboardMarkup::new(btns)).await;
                    }
                } else if action == "set_vid" {
                    if let Some(qual) = Quality::from_callback(target) {
                        settings.default_video = qual;
                        should_save = true;
                    }
                } else if action == "set_aud" {
                    if let Some(qual) = Quality::from_callback(target) {
                        settings.default_audio = qual;
                        should_save = true;
                    }
                } else if action == "settings_main" {
                    should_save = true; // force redraw
                }
                
                if should_save {
                    let key = format!("user_settings:{}", user_id);
                    let val = serde_json::to_string(&settings).unwrap();
                    let res: redis::RedisResult<()> = redis::cmd("SET").arg(&key).arg(val).query_async(&mut conn).await;
                    let _ = res;
                    
                    if let Some(msg) = q.message.as_ref() {
                        use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};
                        let mut keyboard_rows = Vec::new();
                        
                        if settings.auto_download {
                            keyboard_rows.push(vec![InlineKeyboardButton::callback("⚡ Авто", "toggle_mode")]);
                            keyboard_rows.push(vec![InlineKeyboardButton::callback(format!("📹 Видео: {:?}", settings.default_video), "setmenu|vid")]);
                            keyboard_rows.push(vec![InlineKeyboardButton::callback(format!("🎵 Аудио: {:?}", settings.default_audio), "setmenu|aud")]);
                        } else {
                            keyboard_rows.push(vec![InlineKeyboardButton::callback("💬 Ручной", "toggle_mode")]);
                        }
                        
                        keyboard_rows.push(vec![InlineKeyboardButton::callback("💎 Premium", "buy_premium")]);

                        let keyboard = InlineKeyboardMarkup::new(keyboard_rows);
                        let _ = bot.edit_message_reply_markup(msg.chat().id, msg.id()).reply_markup(keyboard).await;
                    }
                }
            }
            bot.answer_callback_query(q.id).await?;
            return Ok(());
        }

        if parts.len() == 2 {
            let action = parts[0];
            let target = parts[1];
            
            if action == "pause" || action == "resume" || action == "abort" {
                if let Some(msg) = q.message {
                    if action == "pause" {
                        task_states.insert(target.to_string(), "paused".to_string()).await;
                        let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![vec![
                            teloxide::types::InlineKeyboardButton::callback("▶️ Продолжить", format!("resume|{}", target)),
                            teloxide::types::InlineKeyboardButton::callback("🛑 Отмена", format!("abort|{}", target)),
                        ]]);
                        let _ = bot.edit_message_reply_markup(msg.chat().id, msg.id()).reply_markup(keyboard).await;
                        let _ = nats.publish_command(&fsocial_common::TaskCommand { task_id: target.to_string(), action: fsocial_common::TaskCommandAction::Pause }).await;
                    } else if action == "resume" {
                        task_states.insert(target.to_string(), "running".to_string()).await;
                        let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![vec![
                            teloxide::types::InlineKeyboardButton::callback("⏸ Пауза", format!("pause|{}", target))
                        ]]);
                        let _ = bot.edit_message_reply_markup(msg.chat().id, msg.id()).reply_markup(keyboard).await;
                        let _ = nats.publish_command(&fsocial_common::TaskCommand { task_id: target.to_string(), action: fsocial_common::TaskCommandAction::Resume }).await;
                    } else if action == "abort" {
                        task_states.insert(target.to_string(), "aborted".to_string()).await;
                        let _ = bot.edit_message_text(msg.chat().id, msg.id(), "🛑 Скачивание прервано пользователем.").reply_markup(teloxide::types::InlineKeyboardMarkup::default()).await;
                        let _ = nats.publish_command(&fsocial_common::TaskCommand { task_id: target.to_string(), action: fsocial_common::TaskCommandAction::Abort }).await;
                    }
                }
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            }

            let quality_str = action;
            let short_id = target;
            
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

                        let mut settings = fsocial_common::UserSettings::default();
                        if let Ok(mut conn) = redis_pool.get().await {
                            let key = format!("user_settings:{}", user_id);
                            let res: redis::RedisResult<String> = redis::cmd("GET").arg(&key).query_async(&mut conn).await;
                            if let Ok(val) = res {
                                if let Ok(parsed) = serde_json::from_str::<fsocial_common::UserSettings>(&val) {
                                    settings = parsed;
                                }
                            }
                        }

                        let now = chrono::Utc::now().timestamp();
                        let is_premium = settings.premium_until.map(|until| until > now).unwrap_or(false);

                        // Check limits if not premium
                        if !is_premium {
                            if let Ok(mut conn) = redis_pool.get().await {
                                let dl_key = format!("today_downloads:{}", user_id);
                                let bytes_key = format!("today_bytes:{}", user_id);
                                
                                let dls: u64 = redis::cmd("GET").arg(&dl_key).query_async(&mut conn).await.unwrap_or(0);
                                let bytes: u64 = redis::cmd("GET").arg(&bytes_key).query_async(&mut conn).await.unwrap_or(0);
                                
                                if dls >= 200 || bytes >= 2147483648 {
                                    let err_msg = "Сегодня лимит бесплатных загрузок исчерпан. Он обновится через 24 часа или можно оформить Premium 💎.";
                                    let _ = bot.send_message(chat, err_msg).await;
                                    return Ok(());
                                }
                            }
                        }

                        // Check if it's a playlist
                        let req = fsocial_common::InfoRequest { url: url_match.url.clone() };
                        if let Ok(info) = nats.request_info(&req).await {
                            if info.is_playlist && !info.playlist_urls.is_empty() {
                                if info.playlist_urls.len() > 50 && !is_premium {
                                    let _ = bot.send_message(chat, "❌ Бесплатные пользователи могут скачивать плейлисты только до 50 треков. Оформите Premium 💎 для снятия ограничений.").await;
                                    return Ok(());
                                }
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
                                    is_premium,
                                );
                                task.status_message_id = Some(msg.id().0);
                                task.status_is_media = msg.regular_message().map(|m| m.photo().is_some() || m.video().is_some() || m.animation().is_some() || m.document().is_some() || m.audio().is_some()).unwrap_or(false);
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
                            is_premium,
                        );
                        task.status_message_id = Some(msg.id().0);
                        task.status_is_media = msg.regular_message().map(|m| m.photo().is_some() || m.video().is_some() || m.animation().is_some() || m.document().is_some() || m.audio().is_some()).unwrap_or(false);

                        let cache_key = format!("file_id:{}:{}", task.quality.callback_id(), task.url);
                        let mut cached_file_id = None;
                        if let Ok(mut conn) = redis_pool.get().await {
                            let res: redis::RedisResult<String> = redis::cmd("GET").arg(&cache_key).query_async(&mut conn).await;
                            if let Ok(fid) = res {
                                cached_file_id = Some(fid);
                            }
                        }

                        if let Some(file_id) = cached_file_id {
                            let input_file = teloxide::types::InputFile::file_id(teloxide::types::FileId(file_id));
                            let bot_watermark = "\n\nСкачано с помощью бота @FSocial_Media_Downloader_bot";
                            
                            // If original message had a media (like thumbnail), edit it. Else edit text or delete and send new.
                            // To keep it simple, we just delete the info message and send the cached file.
                            let _ = bot.delete_message(chat, msg.id()).await;
                            
                            let send_res = if task.quality.is_audio() {
                                bot.send_audio(chat, input_file)
                                    .caption(bot_watermark.trim_start())
                                    .await
                            } else {
                                bot.send_video(chat, input_file)
                                    .caption(bot_watermark)
                                    .await
                            };
                            
                            if send_res.is_ok() {
                                return Ok(());
                            }
                        }

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
