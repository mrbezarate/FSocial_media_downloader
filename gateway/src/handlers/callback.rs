use fsocial_common::{AppConfig, DownloadTask, Quality};
use teloxide::prelude::*;
use tracing::error;

use crate::{UrlCache, nats_client::NatsClient, url_parser};

pub async fn handle(
    bot: crate::MyBot,
    q: CallbackQuery,
    nats: NatsClient,
    _config: AppConfig,
    url_cache: UrlCache,
    task_states: crate::TaskStates,
    redis_pool: deadpool_redis::Pool,
) -> ResponseResult<()> {
    tracing::info!("Received callback query with data: {:?}", q.data);
    if let Some(data) = &q.data {
        let parts: Vec<&str> = data.splitn(2, '|').collect();

        if parts[0] == "buy_premium" {
            let text = "💎 <b>Premium Подписка</b>\n\nВыберите период подписки:\n\n• <b>1 День</b> - 20 ⭐ (попробовать!)\n• <b>1 Месяц</b> - 500 ⭐\n• <b>1 Год</b> - 4800 ⭐ (Выгода 20%!)";
            let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![
                vec![teloxide::types::InlineKeyboardButton::callback("1 День (20 ⭐)", "invoice|day")],
                vec![teloxide::types::InlineKeyboardButton::callback("1 Месяц (500 ⭐)", "invoice|month")],
                vec![teloxide::types::InlineKeyboardButton::callback("1 Год (4800 ⭐)", "invoice|year")],
            ]);

            if let Some(msg) = q.message.as_ref() {
                let _ = bot.edit_message_text(msg.chat().id, msg.id(), text)
                    .reply_markup(keyboard)
                    .parse_mode(teloxide::types::ParseMode::Html)
                    .await;
            }
            let _ = bot.answer_callback_query(q.id).await;
            return Ok(());
        }

        if parts[0] == "invoice" && parts.len() == 2 {
            let period = parts[1];
            let (title, description, payload, amount) = match period {
                "day" => ("Premium на 1 день", "Один день полного доступа.", "premium_1_day", 20),
                "month" => ("Premium на 1 месяц", "Месяц безграничных загрузок.", "premium_1_month", 500),
                "year" => ("Premium на 1 Год", "Год безграничных загрузок со скидкой 20%.", "premium_1_year", 4800),
                _ => return Ok(()),
            };

            let mut final_amount = amount;
            let mut discount_msg = String::new();
            if let Ok(mut conn) = redis_pool.get().await {
                let key = format!("user_settings:{}", q.from.id.0);
                if let Ok(val) = redis::cmd("GET").arg(&key).query_async::<String>(&mut conn).await {
                    if let Ok(settings) = serde_json::from_str::<fsocial_common::UserSettings>(&val) {
                        if settings.active_discount_percent > 0 {
                            final_amount = (amount as f64 * (1.0 - (settings.active_discount_percent as f64 / 100.0))) as u64;
                            discount_msg = format!(" (Скидка {}%!)", settings.active_discount_percent);
                        }
                    }
                }
            }

            let prices = vec![teloxide::types::LabeledPrice {
                label: format!("Premium{}", discount_msg),
                amount: final_amount as u32,
            }];

            if let Some(msg) = q.message.as_ref() {
                let _ = bot
                    .send_invoice(msg.chat().id, title, description, payload, "XTR", prices)
                    .await;
            }
            let _ = bot.answer_callback_query(q.id).await;
            return Ok(());
        }

        if parts[0].starts_with("set_")
            || parts[0].starts_with("setmenu")
            || parts[0].starts_with("settings")
            || parts[0] == "toggle_mode"
        {
            let action = parts[0];
            let target = if parts.len() > 1 { parts[1] } else { "" };
            let user_id = q.from.id.0;

            let mut settings = fsocial_common::UserSettings::default();
            if let Ok(mut conn) = redis_pool.get().await {
                let key = format!("user_settings:{}", user_id);
                let res: redis::RedisResult<String> =
                    redis::cmd("GET").arg(&key).query_async(&mut conn).await;
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
                                    row.push(InlineKeyboardButton::callback(
                                        format!("{:?}", q_opt),
                                        format!("set_vid|{}", q_opt.callback_id()),
                                    ));
                                }
                                btns.push(row);
                            }
                        } else if target == "aud" {
                            for chunk in fsocial_common::Quality::audio_options().chunks(2) {
                                let mut row = Vec::new();
                                for q_opt in chunk {
                                    row.push(InlineKeyboardButton::callback(
                                        format!("{:?}", q_opt),
                                        format!("set_aud|{}", q_opt.callback_id()),
                                    ));
                                }
                                btns.push(row);
                            }
                        }
                        btns.push(vec![InlineKeyboardButton::callback(
                            "[ Назад ]",
                            "settings_main",
                        )]);
                        let _ = bot
                            .edit_message_reply_markup(msg.chat().id, msg.id())
                            .reply_markup(InlineKeyboardMarkup::new(btns))
                            .await;
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
                    let res: redis::RedisResult<()> = redis::cmd("SET")
                        .arg(&key)
                        .arg(val)
                        .query_async(&mut conn)
                        .await;
                    let _ = res;

                    if let Some(msg) = q.message.as_ref() {
                        use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};
                        let mut keyboard_rows = Vec::new();

                        if settings.auto_download {
                            keyboard_rows.push(vec![InlineKeyboardButton::callback(
                                "⚡ Авто",
                                "toggle_mode",
                            )]);
                            keyboard_rows.push(vec![InlineKeyboardButton::callback(
                                format!("📹 Видео: {:?}", settings.default_video),
                                "setmenu|vid",
                            )]);
                            keyboard_rows.push(vec![InlineKeyboardButton::callback(
                                format!("🎵 Аудио: {:?}", settings.default_audio),
                                "setmenu|aud",
                            )]);
                        } else {
                            keyboard_rows.push(vec![InlineKeyboardButton::callback(
                                "💬 Ручной",
                                "toggle_mode",
                            )]);
                        }

                        keyboard_rows.push(vec![InlineKeyboardButton::callback(
                            "💎 Premium",
                            "buy_premium",
                        )]);

                        let keyboard = InlineKeyboardMarkup::new(keyboard_rows);
                        let _ = bot
                            .edit_message_reply_markup(msg.chat().id, msg.id())
                            .reply_markup(keyboard)
                            .await;
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
                        task_states
                            .insert(target.to_string(), "paused".to_string())
                            .await;
                        let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![vec![
                            teloxide::types::InlineKeyboardButton::callback(
                                "▶️ Продолжить",
                                format!("resume|{}", target),
                            ),
                            teloxide::types::InlineKeyboardButton::callback(
                                "🛑 Отмена",
                                format!("abort|{}", target),
                            ),
                        ]]);
                        let _ = bot
                            .edit_message_reply_markup(msg.chat().id, msg.id())
                            .reply_markup(keyboard)
                            .await;
                        let _ = nats
                            .publish_command(&fsocial_common::TaskCommand {
                                task_id: target.to_string(),
                                action: fsocial_common::TaskCommandAction::Pause,
                            })
                            .await;
                    } else if action == "resume" {
                        task_states
                            .insert(target.to_string(), "running".to_string())
                            .await;
                        let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![vec![
                            teloxide::types::InlineKeyboardButton::callback(
                                "⏸ Пауза",
                                format!("pause|{}", target),
                            ),
                        ]]);
                        let _ = bot
                            .edit_message_reply_markup(msg.chat().id, msg.id())
                            .reply_markup(keyboard)
                            .await;
                        let _ = nats
                            .publish_command(&fsocial_common::TaskCommand {
                                task_id: target.to_string(),
                                action: fsocial_common::TaskCommandAction::Resume,
                            })
                            .await;
                    } else if action == "abort" {
                        task_states
                            .insert(target.to_string(), "aborted".to_string())
                            .await;
                        let _ = bot
                            .edit_message_text(
                                msg.chat().id,
                                msg.id(),
                                "🛑 Скачивание прервано пользователем.",
                            )
                            .reply_markup(teloxide::types::InlineKeyboardMarkup::default())
                            .await;
                        let _ = nats
                            .publish_command(&fsocial_common::TaskCommand {
                                task_id: target.to_string(),
                                action: fsocial_common::TaskCommandAction::Abort,
                            })
                            .await;
                    }
                }
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            }

            if action == "admin" {
                if let Some(msg) = q.message {
                    match target {
                        "stats" => {
                            if let Ok(mut conn) = redis_pool.get().await {
                                let keys: Vec<String> = redis::cmd("KEYS").arg("user_settings:*").query_async(&mut conn).await.unwrap_or_default();
                                let downloads: i64 = redis::cmd("GET").arg("total_downloads_global").query_async(&mut conn).await.unwrap_or(0);
                                let text = format!("📊 <b>Статистика системы:</b>\n\n👥 Всего пользователей: {}\n💾 Всего загрузок: {}", keys.len(), downloads);
                                let _ = bot.edit_message_text(msg.chat().id, msg.id(), text).parse_mode(teloxide::types::ParseMode::Html).await;
                            }
                        }
                        "logs" => {
                            let mut logs_text = String::new();
                            if let Ok(buf) = crate::admin_logs::LOG_BUFFER.read() {
                                let last_20 = buf.iter().rev().take(20).collect::<Vec<_>>();
                                for log in last_20.iter().rev() {
                                    logs_text.push_str(log);
                                    logs_text.push('\n');
                                }
                            }
                            if logs_text.is_empty() {
                                logs_text = "Логи пусты.".to_string();
                            }
                            let text = format!("📜 <b>Последние логи (20 строк):</b>\n\n<pre>{}</pre>", logs_text);
                            
                            let mut is_streaming = false;
                            if let Ok(mut conn) = redis_pool.get().await {
                                let stream_val: Option<String> = redis::cmd("GET").arg("admin:log_stream_chat").query_async(&mut conn).await.unwrap_or(None);
                                is_streaming = stream_val.is_some();
                            }
                            let stream_btn = if is_streaming {
                                teloxide::types::InlineKeyboardButton::callback("⏹ Выключить стриминг логов в чат", "admin|stop_log_stream")
                            } else {
                                teloxide::types::InlineKeyboardButton::callback("▶️ Включить стриминг логов в чат", "admin|start_log_stream")
                            };
                            let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![vec![stream_btn]]);

                            let _ = bot.edit_message_text(msg.chat().id, msg.id(), text).reply_markup(keyboard).parse_mode(teloxide::types::ParseMode::Html).await;
                        }
                        "start_log_stream" => {
                            if let Ok(mut conn) = redis_pool.get().await {
                                let _: redis::RedisResult<()> = redis::cmd("SET").arg("admin:log_stream_chat").arg(msg.chat().id.0).query_async(&mut conn).await;
                                let _ = bot.edit_message_text(msg.chat().id, msg.id(), "▶️ Стриминг логов включён. Новые логи будут приходить в этот чат каждые 3 секунды.").await;
                            }
                        }
                        "stop_log_stream" => {
                            if let Ok(mut conn) = redis_pool.get().await {
                                let _: redis::RedisResult<()> = redis::cmd("DEL").arg("admin:log_stream_chat").query_async(&mut conn).await;
                                let _ = bot.edit_message_text(msg.chat().id, msg.id(), "⏹ Стриминг логов выключен.").await;
                            }
                        }
                        "promo" => {
                            if let Ok(mut conn) = redis_pool.get().await {
                                let _: redis::RedisResult<()> = redis::cmd("SET").arg(format!("admin_state:{}", msg.chat().id.0)).arg("waiting_promo_create").query_async(&mut conn).await;
                                let text = "🎟 <b>Создание промокода</b>\n\nОтправьте параметры через пробел:\n<code>КОД ТИП ЗНАЧЕНИЕ ИСПОЛЬЗОВАНИЯ</code>\n\nТипы:\n• <code>days</code> (выдача бесплатных дней)\n• <code>discount</code> (скидка в % на покупку)\n\nПримеры:\n<code>VIP2024 days 30 100</code> (30 дней, 100 раз)\n<code>SALE50 discount 50 100</code> (Скидка 50%, 100 раз)\n\nДля отмены введите /cancel";
                                let _ = bot.edit_message_text(msg.chat().id, msg.id(), text).parse_mode(teloxide::types::ParseMode::Html).await;
                            }
                        }
                        "admins_menu" => {
                            let text = "👑 <b>Управление администраторами</b>\n\nВыберите действие:";
                            let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![
                                vec![
                                    teloxide::types::InlineKeyboardButton::callback("➕ Добавить админа", "admin|add_admin"),
                                    teloxide::types::InlineKeyboardButton::callback("➖ Удалить админа", "admin|remove_admin"),
                                ],
                            ]);
                            let _ = bot.edit_message_text(msg.chat().id, msg.id(), text).reply_markup(keyboard).parse_mode(teloxide::types::ParseMode::Html).await;
                        }
                        "add_admin" => {
                            if let Ok(mut conn) = redis_pool.get().await {
                                let _: redis::RedisResult<()> = redis::cmd("SET").arg(format!("admin_state:{}", msg.chat().id.0)).arg("waiting_admin_add").query_async(&mut conn).await;
                                let text = "Отправьте <b>Telegram ID</b> пользователя, которого хотите назначить администратором:\n\nДля отмены введите /cancel";
                                let _ = bot.edit_message_text(msg.chat().id, msg.id(), text).parse_mode(teloxide::types::ParseMode::Html).await;
                            }
                        }
                        "remove_admin" => {
                            if let Ok(mut conn) = redis_pool.get().await {
                                let _: redis::RedisResult<()> = redis::cmd("SET").arg(format!("admin_state:{}", msg.chat().id.0)).arg("waiting_admin_remove").query_async(&mut conn).await;
                                let text = "Отправьте <b>Telegram ID</b> пользователя, у которого хотите забрать права администратора:\n\nДля отмены введите /cancel";
                                let _ = bot.edit_message_text(msg.chat().id, msg.id(), text).parse_mode(teloxide::types::ParseMode::Html).await;
                            }
                        }
                        "reklama" => {
                            if let Ok(mut conn) = redis_pool.get().await {
                                let _: redis::RedisResult<()> = redis::cmd("SET").arg(format!("admin_state:{}", msg.chat().id.0)).arg("waiting_broadcast").query_async(&mut conn).await;
                                let text = "📢 <b>Массовая рассылка</b>\n\nОтправьте текст, который хотите разослать <b>всем</b> пользователям бота:\n\nДля отмены введите /cancel";
                                let _ = bot.edit_message_text(msg.chat().id, msg.id(), text).parse_mode(teloxide::types::ParseMode::Html).await;
                            }
                        }
                        _ => {}
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
                            // we moved edit_message_text down after task generation

                            let user_id = q.from.id.0;
                            let chat_id = msg.chat().id.0;
                            let message_id = msg.id().0;
                            let chat = msg.chat().id;

                            let mut settings = fsocial_common::UserSettings::default();
                            if let Ok(mut conn) = redis_pool.get().await {
                                let key = format!("user_settings:{}", user_id);
                                let res: redis::RedisResult<String> =
                                    redis::cmd("GET").arg(&key).query_async(&mut conn).await;
                                if let Ok(val) = res {
                                    if let Ok(parsed) =
                                        serde_json::from_str::<fsocial_common::UserSettings>(&val)
                                    {
                                        settings = parsed;
                                    }
                                }
                            }

                            let now = chrono::Utc::now().timestamp();
                            let is_premium = settings
                                .premium_until
                                .map(|until| until > now)
                                .unwrap_or(false);

                            // Check limits if not premium
                            if !is_premium {
                                if let Ok(mut conn) = redis_pool.get().await {
                                    let dl_key = format!("today_downloads:{}", user_id);
                                    let bytes_key = format!("today_bytes:{}", user_id);

                                    let dls: u64 = redis::cmd("GET")
                                        .arg(&dl_key)
                                        .query_async(&mut conn)
                                        .await
                                        .unwrap_or(0);
                                    let bytes: u64 = redis::cmd("GET")
                                        .arg(&bytes_key)
                                        .query_async(&mut conn)
                                        .await
                                        .unwrap_or(0);

                                    if dls >= 200 || bytes >= 2147483648 {
                                        let err_msg = "Сегодня лимит бесплатных загрузок исчерпан. Он обновится через 24 часа или можно оформить Premium 💎.";
                                        let _ = bot.send_message(chat, err_msg).await;
                                        return Ok(());
                                    }
                                }
                            }

                            // Check if it's a playlist
                            let req = fsocial_common::InfoRequest {
                                url: url_match.url.clone(),
                            };
                            if let Ok(info) = nats.request_info(&req).await {
                                if info.is_playlist && !info.playlist_urls.is_empty() {
                                    if info.playlist_urls.len() > 50 && !is_premium {
                                        let _ = bot.send_message(chat, "❌ Бесплатные пользователи могут скачивать плейлисты только до 50 треков. Оформите Premium 💎 для снятия ограничений.").await;
                                        return Ok(());
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
                                    task.status_is_media = msg
                                        .regular_message()
                                        .map(|m| {
                                            m.photo().is_some()
                                                || m.video().is_some()
                                                || m.animation().is_some()
                                                || m.document().is_some()
                                                || m.audio().is_some()
                                        })
                                        .unwrap_or(false);
                                    task.playlist_urls = Some(info.playlist_urls.clone());

                                    let playlist_status = format!(
                                        "⏳ Скачивание плейлиста: 0/{}\nПрогресс: 0%",
                                        info.playlist_urls.len()
                                    );
                                    let keyboard =
                                        teloxide::types::InlineKeyboardMarkup::new(vec![vec![
                                            teloxide::types::InlineKeyboardButton::callback(
                                                "🛑 Отмена",
                                                format!("abort|{}", task.task_id),
                                            ),
                                        ]]);

                                    if let Err(_) = bot
                                        .edit_message_text(chat, msg.id(), playlist_status.clone())
                                        .reply_markup(keyboard.clone())
                                        .await
                                    {
                                        let _ = bot
                                            .edit_message_caption(chat, msg.id())
                                            .caption(playlist_status)
                                            .reply_markup(keyboard)
                                            .await;
                                    }
                                    let _ = nats.publish_task(&task).await;
                                    return Ok(());
                                }
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
                            task.status_is_media = msg
                                .regular_message()
                                .map(|m| {
                                    m.photo().is_some()
                                        || m.video().is_some()
                                        || m.animation().is_some()
                                        || m.document().is_some()
                                        || m.audio().is_some()
                                })
                                .unwrap_or(false);

                            let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![vec![
                                teloxide::types::InlineKeyboardButton::callback(
                                    "🛑 Отмена",
                                    format!("abort|{}", task.task_id),
                                ),
                            ]]);

                            if let Err(_) = bot
                                .edit_message_text(msg.chat().id, msg.id(), "⏳ Загружаю...")
                                .reply_markup(keyboard.clone())
                                .await
                            {
                                let _ = bot
                                    .edit_message_caption(msg.chat().id, msg.id())
                                    .caption("⏳ Загружаю...")
                                    .reply_markup(keyboard)
                                    .await;
                            }

                            let cache_key =
                                format!("file_id:{}:{}", task.quality.callback_id(), task.url);
                            let mut cached_file_id = None;
                            if let Ok(mut conn) = redis_pool.get().await {
                                let res: redis::RedisResult<String> = redis::cmd("GET")
                                    .arg(&cache_key)
                                    .query_async(&mut conn)
                                    .await;
                                if let Ok(fid) = res {
                                    cached_file_id = Some(fid);
                                }
                            }

                            if let Some(file_id) = cached_file_id {
                                let input_file = teloxide::types::InputFile::file_id(
                                    teloxide::types::FileId(file_id),
                                );
                                let bot_watermark =
                                    "\n\nСкачано с помощью бота @FSocial_Media_Downloader_bot";

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
                                if let Err(_) = bot
                                    .edit_message_text(
                                        msg.chat().id,
                                        msg.id(),
                                        "❌ Внутренняя ошибка",
                                    )
                                    .await
                                {
                                    let _ = bot
                                        .edit_message_caption(msg.chat().id, msg.id())
                                        .caption("❌ Внутренняя ошибка")
                                        .await;
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
