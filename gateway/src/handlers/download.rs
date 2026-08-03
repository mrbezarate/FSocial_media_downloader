use fsocial_common::{AppConfig, DownloadTask};
use teloxide::prelude::*;

use crate::{UrlCache, nats_client::NatsClient, url_parser};

const SUPPORTED_SOURCES_INFO: &str = "\
❌ <b>Ошибка: Ссылка недействительна или источник не поддерживается.</b>\n\n\
Пожалуйста, отправляйте только рабочие ссылки со следующих платформ:\n\n\
🎬 <b>Видео:</b>\n\
• YouTube\n\
• Instagram\n\
• TikTok\n\
• Pinterest\n\n\
🎵 <b>Аудио:</b>\n\
• Spotify\n\
• SoundCloud";

pub async fn handle(
    bot: crate::MyBot,
    msg: Message,
    nats: NatsClient,
    config: AppConfig,
    url_cache: UrlCache,
    redis_pool: deadpool_redis::Pool,
) -> ResponseResult<()> {
    let text = if let Some(t) = msg.text().or(msg.caption()) {
        t
    } else {
        return Ok(());
    };

    let url_match = if let Some(m) = url_parser::detect(text) {
        m
    } else {
        if msg.chat.is_private() {
            // Deletes the user's message containing unsupported/invalid link
            let _ = bot.delete_message(msg.chat.id, msg.id).await;
            let _ = bot
                .send_message(msg.chat.id, SUPPORTED_SOURCES_INFO)
                .parse_mode(teloxide::types::ParseMode::Html)
                .await;
        }
        return Ok(());
    };

    let is_group = msg.chat.is_group() || msg.chat.is_supergroup();
    let user_id = msg.from.as_ref().map(|u| u.id.0).unwrap_or(0);
    let chat_id = msg.chat.id.0;
    let message_id = msg.id.0;

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
    }

    let now = chrono::Utc::now().timestamp();
    let is_premium = settings
        .premium_until
        .map(|until| until > now)
        .unwrap_or(false);

    if !is_premium && user_id > 0 {
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
                let _ = bot.send_message(msg.chat.id, "Сегодня лимит бесплатных загрузок исчерпан. Он обновится через 24 часа или можно оформить Premium.")
                    .reply_parameters(teloxide::types::ReplyParameters::new(msg.id))
                    .await;
                return Ok(());
            }
        }
    }
    let bypass_info = is_group
        || url_match.platform == fsocial_common::Platform::Pinterest
        || settings.auto_download;

    if bypass_info {
        let default_quality = if url_match.platform == fsocial_common::Platform::Pinterest {
            fsocial_common::Quality::Best
        } else if url_match.media_type == fsocial_common::MediaType::Audio
            || url_match.platform == fsocial_common::Platform::Spotify
            || url_match.platform == fsocial_common::Platform::SoundCloud
        {
            settings.default_audio
        } else {
            settings.default_video
        };

        let mut task = DownloadTask::new(
            url_match.url,
            url_match.platform,
            url_match.media_type,
            default_quality,
            chat_id,
            message_id,
            user_id,
            is_group,
            is_premium,
        );
        task.reply_to_message_id = Some(message_id);

        let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![vec![
            teloxide::types::InlineKeyboardButton::callback(
                "🛑 Отмена",
                format!("abort|{}", task.task_id),
            ),
        ]]);

        let status_msg = bot
            .send_message(msg.chat.id, "⏳ Загружаю...")
            .reply_parameters(teloxide::types::ReplyParameters::new(msg.id))
            .reply_markup(keyboard)
            .await?;

        task.status_message_id = Some(status_msg.id.0);

        let cached_file_id = crate::utils::get_cached_file_id(&redis_pool, task.quality.callback_id(), &task.url).await;

        if let Some(file_id) = cached_file_id {
            let input_file = teloxide::types::InputFile::file_id(teloxide::types::FileId(file_id));
            let bot_watermark = crate::utils::BOT_WATERMARK;
            let send_res = if task.media_type == fsocial_common::MediaType::Audio
                || task.platform == fsocial_common::Platform::Spotify
                || task.platform == fsocial_common::Platform::SoundCloud
            {
                bot.send_audio(msg.chat.id, input_file)
                    .caption(bot_watermark.trim_start())
                    .reply_parameters(teloxide::types::ReplyParameters::new(msg.id))
                    .await
            } else {
                bot.send_video(msg.chat.id, input_file)
                    .caption(bot_watermark)
                    .reply_parameters(teloxide::types::ReplyParameters::new(msg.id))
                    .await
            };

            if send_res.is_ok() {
                let _ = bot.delete_message(msg.chat.id, status_msg.id).await;
                return Ok(());
            }
            // If it failed (e.g. file_id invalid), we fallback to download
        }

        if let Err(e) = nats.publish_task(&task).await {
            tracing::error!("Failed to publish task: {}", e);
            bot.edit_message_text(msg.chat.id, status_msg.id, "❌ Ошибка")
                .await?;
        }
    } else {
        let req = fsocial_common::InfoRequest {
            url: url_match.url.clone(),
        };
        let short_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        url_cache
            .insert(short_id.clone(), url_match.url.clone())
            .await;

        // Мгновенный отклик, чтобы пользователь видел, что бот работает, даже если кидает несколько ссылок
        let processing_msg = bot
            .send_message(msg.chat.id, "⏳ <i>Анализирую ссылку...</i>")
            .parse_mode(teloxide::types::ParseMode::Html)
            .reply_parameters(teloxide::types::ReplyParameters::new(msg.id))
            .await;

        match nats.request_info(&req).await {
            Ok(info) => {
                if let Ok(pm) = &processing_msg {
                    let _ = bot.delete_message(msg.chat.id, pm.id).await;
                }

                if info.is_playlist && info.playlist_count.unwrap_or(0) > 50 && !is_premium {
                    let _ = bot.send_message(msg.chat.id, "❌ Бесплатные пользователи могут скачивать плейлисты только до 50 треков. Оформите Premium для снятия ограничений.")
                        .reply_parameters(teloxide::types::ReplyParameters::new(msg.id))
                        .await;
                    return Ok(());
                }

                let max_size = if config.is_local_api() { 2000.0 } else { 50.0 };
                let text = crate::ui::UiBuilder::build_info_message(&info, max_size, is_premium);
                let keyboard =
                    crate::ui::UiBuilder::build_quality_keyboard(&info, &short_id, max_size, is_premium);

                let mut sent_photo = false;
                if let Some(thumb_url) = info.thumbnail {
                    if let Ok(url) = reqwest::Url::parse(&thumb_url) {
                        let res = bot
                            .send_photo(msg.chat.id, teloxide::types::InputFile::url(url))
                            .caption(text.clone())
                            .parse_mode(teloxide::types::ParseMode::Html)
                            .reply_markup(keyboard.clone())
                            .reply_parameters(teloxide::types::ReplyParameters::new(msg.id))
                            .await;
                        if res.is_ok() {
                            sent_photo = true;
                        }
                    }
                }

                if !sent_photo {
                    let _ = bot
                        .send_message(msg.chat.id, text)
                        .parse_mode(teloxide::types::ParseMode::Html)
                        .reply_markup(keyboard)
                        .reply_parameters(teloxide::types::ReplyParameters::new(msg.id))
                        .await;
                }

                return Ok(());
            }
            Err(e) => {
                if let Ok(pm) = &processing_msg {
                    let _ = bot.delete_message(msg.chat.id, pm.id).await;
                }

                tracing::error!("request_info failed: {}", e);
                // Link is broken, private or invalid -> delete user's message and inform
                let _ = bot.delete_message(msg.chat.id, msg.id).await;

                let e_str = e.to_string();
                let err_msg = if e_str.contains("Скачивание прямых")
                    || e_str.contains("Скачивание фото")
                {
                    format!("❌ <b>Ошибка:</b>\n<i>{}</i>", e_str)
                } else {
                    format!(
                        "❌ <b>Не удалось обработать ссылку!</b>\n<i>Причина: {}</i>\n\n{}",
                        e_str, SUPPORTED_SOURCES_INFO
                    )
                };

                let _ = bot
                    .send_message(msg.chat.id, err_msg)
                    .parse_mode(teloxide::types::ParseMode::Html)
                    .await;
            }
        }
    }

    Ok(())
}
