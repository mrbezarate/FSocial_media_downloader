use fsocial_common::{AppConfig, TaskResult, TaskStatus};
use futures::StreamExt;
use std::path::PathBuf;
use teloxide::{prelude::*, types::InputFile};
use tracing::{error, info};

use crate::nats_client::NatsClient;

pub async fn listen(
    bot: crate::MyBot,
    nats: NatsClient,
    config: AppConfig,
    task_states: crate::TaskStates,
    redis_pool: deadpool_redis::Pool,
) {
    let mut results_sub = nats.subscribe_results().await.expect("Results sub failed");
    let mut progress_sub = nats
        .subscribe_progress()
        .await
        .expect("Progress sub failed");

    loop {
        tokio::select! {
            Some(msg) = results_sub.next() => {
                if let Ok(res) = serde_json::from_slice::<TaskResult>(&msg.payload) {
                    let bot_clone = bot.clone();
                    let config_clone = config.clone();
                    let redis_pool_clone = redis_pool.clone();
                    let nats_clone = nats.clone();
                    tokio::spawn(async move {
                        handle_result(&bot_clone, res, &config_clone, &redis_pool_clone, &nats_clone).await;
                    });
                }
            }
            Some(msg) = progress_sub.next() => {
                if let Ok(res) = serde_json::from_slice::<TaskResult>(&msg.payload) {
                    handle_progress(&bot, res, &task_states).await;
                }
            }
        }
    }
}

async fn handle_result(
    bot: &crate::MyBot,
    res: TaskResult,
    config: &AppConfig,
    redis_pool: &deadpool_redis::Pool,
    nats: &crate::NatsClient,
) {
    let chat_id = teloxide::types::ChatId(res.chat_id);

    // DECR active tasks if the status is terminal
    let is_terminal = match &res.status {
        TaskStatus::Completed { .. } => true,
        TaskStatus::PlaylistCompleted { .. } => true,
        TaskStatus::V2Completed { .. } => true,
        TaskStatus::Failed { retryable, .. } => !retryable,
        _ => false,
    };

    if is_terminal {
        if let Ok(mut conn) = redis_pool.get().await {
            let _: () = redis::cmd("DECR")
                .arg(&format!("active_tasks:{}", res.user_id))
                .query_async(&mut conn)
                .await
                .unwrap_or(());
        }
    }

    match res.status {
        TaskStatus::Completed {
            file_path,
            title,
            is_audio,
            performer,
            thumb_path,
            cache_key,
            ..
        } => {
            if config.use_v2_only {
                tracing::error!("Legacy Completed status received but USE_V2_ONLY is enabled. Skipping.");
                return;
            }
            let path = PathBuf::from(&file_path);

            if let Ok(meta) = tokio::fs::metadata(&path).await {
                let size_mb = meta.len() as f64 / (1024.0 * 1024.0);
                if size_mb > 50.0 && !config.is_local_api() {
                    if let Some(msg_id) = res.status_message_id {
                        let _ = bot
                            .delete_message(chat_id, teloxide::types::MessageId(msg_id))
                            .await;
                    }
                    let _ = bot.send_message(
                        chat_id,
                        format!(
                            "❌ <b>Файл слишком большой ({:.1} МБ)</b>\n\nОфициальный Telegram Bot API ограничил отправку файлов до 50 МБ.\n\n💡 <i>Выберите меньшее качество (720p/480p) или попробуйте скачать аудио.</i>",
                            size_mb
                        )
                    )
                    .parse_mode(teloxide::types::ParseMode::Html)
                    .await;

                    let _ = tokio::fs::remove_file(&file_path).await;
                    if let Some(thumb) = &thumb_path {
                        let _ = tokio::fs::remove_file(thumb).await;
                    }
                    return;
                }
            }
            let _file_name = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();

            let mut edit_success = false;
            let mut edit_res_msg = None;

            if res.status_is_media && !is_audio {
                if let Some(msg_id) = res.status_message_id {
                    let mid = teloxide::types::MessageId(msg_id);
                    let input_file = if config.is_local_api() {
                        let abs_path = tokio::fs::canonicalize(&path).await.unwrap_or_else(|_| path.clone());
                        InputFile::file_id(format!("file://{}", abs_path.to_string_lossy()).into())
                    } else {
                        InputFile::file(path.clone())
                    };

                    let bot_watermark = crate::utils::BOT_WATERMARK;
                    let media = if is_audio {
                        let mut aud = teloxide::types::InputMediaAudio::new(input_file)
                            .title(title.clone())
                            .caption(bot_watermark.trim_start().to_string());
                        if let Some(perf) = &performer {
                            aud.performer = Some(perf.clone());
                        }
                        if let Some(thumb) = &thumb_path {
                            let thumb_file = if config.is_local_api() {
                                let abs_thumb = tokio::fs::canonicalize(thumb).await.unwrap_or_else(|_| PathBuf::from(thumb));
                                InputFile::file_id(
                                    format!("file://{}", abs_thumb.to_string_lossy()).into(),
                                )
                            } else {
                                InputFile::file(thumb.clone())
                            };
                            aud.thumbnail = Some(thumb_file);
                        }
                        teloxide::types::InputMedia::Audio(aud)
                    } else {
                        let path_ext = path
                            .extension()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .to_lowercase();
                        if path_ext == "gif" {
                            teloxide::types::InputMedia::Animation(
                                teloxide::types::InputMediaAnimation::new(input_file)
                                    .caption(bot_watermark.to_string()),
                            )
                        } else if path_ext == "jpg"
                            || path_ext == "jpeg"
                            || path_ext == "png"
                            || path_ext == "webp"
                        {
                            teloxide::types::InputMedia::Photo(
                                teloxide::types::InputMediaPhoto::new(input_file)
                                    .caption(bot_watermark.to_string()),
                            )
                        } else {
                            let mut vid = teloxide::types::InputMediaVideo::new(input_file)
                                .caption(bot_watermark.to_string());
                            if let Some(thumb) = &thumb_path {
                                let thumb_file = if config.is_local_api() {
                                    let abs_thumb = tokio::fs::canonicalize(thumb).await.unwrap_or_else(|_| PathBuf::from(thumb));
                                    InputFile::file_id(
                                        format!("file://{}", abs_thumb.to_string_lossy()).into(),
                                    )
                                } else {
                                    InputFile::file(thumb.clone())
                                };
                                vid.thumbnail = Some(thumb_file);
                            }
                            teloxide::types::InputMedia::Video(vid)
                        }
                    };

                    if let Ok(m) = bot.edit_message_media(chat_id, mid, media).await {
                        edit_success = true;
                        edit_res_msg = Some(m);
                    }
                }
            }

            let send_result = if edit_success {
                Ok(edit_res_msg.unwrap())
            } else {
                let input_file = if config.is_local_api() {
                    let abs_path = tokio::fs::canonicalize(&path).await.unwrap_or_else(|_| path.clone());
                    InputFile::file_id(format!("file://{}", abs_path.to_string_lossy()).into())
                } else {
                    InputFile::file(path.clone())
                };

                let bot_watermark = crate::utils::BOT_WATERMARK;
                let api_res = if is_audio {
                    let mut req = bot
                        .send_audio(chat_id, input_file)
                        .title(title.clone())
                        .caption(bot_watermark.trim_start().to_string());
                    if let Some(perf) = &performer {
                        req = req.performer(perf.clone());
                    }
                    if let Some(thumb) = &thumb_path {
                        let thumb_file = if config.is_local_api() {
                            let abs_thumb = tokio::fs::canonicalize(thumb).await.unwrap_or_else(|_| PathBuf::from(thumb));
                            InputFile::file_id(
                                format!("file://{}", abs_thumb.to_string_lossy()).into(),
                            )
                        } else {
                            InputFile::file(thumb.clone())
                        };
                        req = req.thumbnail(thumb_file);
                    }
                    if let Some(reply_id) = res.reply_to_message_id {
                        req = req.reply_parameters(teloxide::types::ReplyParameters::new(
                            teloxide::types::MessageId(reply_id),
                        ));
                    }
                    req.await
                } else {
                    let path_ext = path
                        .extension()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_lowercase();
                    if path_ext == "jpg"
                        || path_ext == "jpeg"
                        || path_ext == "png"
                        || path_ext == "webp"
                    {
                        let mut req = bot
                            .send_photo(chat_id, input_file)
                            .caption(bot_watermark.to_string());
                        if let Some(reply_id) = res.reply_to_message_id {
                            req = req.reply_parameters(teloxide::types::ReplyParameters::new(
                                teloxide::types::MessageId(reply_id),
                            ));
                        }
                        req.await
                    } else if path_ext == "gif" {
                        let mut req = bot
                            .send_animation(chat_id, input_file)
                            .caption(bot_watermark.to_string());
                        if let Some(reply_id) = res.reply_to_message_id {
                            req = req.reply_parameters(teloxide::types::ReplyParameters::new(
                                teloxide::types::MessageId(reply_id),
                            ));
                        }
                        req.await
                    } else {
                        let mut req = bot
                            .send_video(chat_id, input_file)
                            .caption(bot_watermark.to_string());
                        if let Some(thumb) = &thumb_path {
                            let thumb_file = if config.is_local_api() {
                                let abs_thumb = tokio::fs::canonicalize(thumb).await.unwrap_or_else(|_| PathBuf::from(thumb));
                                InputFile::file_id(
                                    format!("file://{}", abs_thumb.to_string_lossy()).into(),
                                )
                            } else {
                                InputFile::file(thumb.clone())
                            };
                            req = req.thumbnail(thumb_file);
                        }
                        if let Some(reply_id) = res.reply_to_message_id {
                            req = req.reply_parameters(teloxide::types::ReplyParameters::new(
                                teloxide::types::MessageId(reply_id),
                            ));
                        }
                        req.await
                    }
                };
                api_res
            };

            match send_result {
                Ok(_) => {
                    info!("Successfully sent file to chat {}", chat_id);
                    if !edit_success {
                        if let Some(msg_id) = res.status_message_id {
                            let mid = teloxide::types::MessageId(msg_id);
                            let _ = bot.delete_message(chat_id, mid).await;
                        }
                    }
                    if let Err(e) = tokio::fs::remove_file(&file_path).await {
                        error!("Failed to clean up file {}: {}", file_path, e);
                    }
                    if let Some(thumb) = &thumb_path {
                        let _ = tokio::fs::remove_file(thumb).await;
                    }

                    // Cache the file_id in Redis
                    if let Some(key) = cache_key {
                        let extracted_file_id =
                            if let Some(m) = send_result.as_ref().unwrap().video() {
                                Some(m.file.id.clone())
                            } else if let Some(m) = send_result.as_ref().unwrap().audio() {
                                Some(m.file.id.clone())
                            } else if let Some(m) =
                                send_result.as_ref().unwrap().photo().and_then(|p| p.last())
                            {
                                Some(m.file.id.clone())
                            } else if let Some(m) = send_result.as_ref().unwrap().animation() {
                                Some(m.file.id.clone())
                            } else {
                                None
                            };

                        if let Some(file_id) = extracted_file_id {
                            if let Ok(mut conn) = redis_pool.get().await {
                                let _: () = redis::cmd("SETEX")
                                    .arg(&key)
                                    .arg(30 * 24 * 3600) // 30 days
                                    .arg(&file_id.0)
                                    .query_async(&mut conn)
                                    .await
                                    .unwrap_or(());
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to send file: {}", e);
                    if let Some(msg_id) = res.status_message_id {
                        let mid = teloxide::types::MessageId(msg_id);
                        let err_msg = format!("❌ Ошибка отправки: {}", e);
                        if let Err(_) = crate::utils::edit_message_text_or_caption(&bot, chat_id, mid, err_msg).await {
                        }
                    }
                }
            }
        }
        TaskStatus::PlaylistCompleted {
            files,
            playlist_title: _,
            failed_count,
            failed_items,
        } => {
            if config.use_v2_only {
                tracing::error!("Legacy PlaylistCompleted status received but USE_V2_ONLY is enabled. Skipping.");
                return;
            }
            tracing::info!(
                "Received PlaylistCompleted for chat {} with {} files",
                chat_id,
                files.len()
            );
            use teloxide::types::{InputMedia, InputMediaAudio, InputMediaVideo};

            // Chunk files into groups of 10
            for chunk in files.chunks(10) {
                let mut media_group = Vec::new();
                for (
                    file_path,
                    title,
                    _duration_secs,
                    performer,
                    thumb_path,
                    is_audio,
                    _cache_key,
                ) in chunk
                {
                    let path = PathBuf::from(file_path);
                    let input_file = if config.is_local_api() {
                        let abs_path = tokio::fs::canonicalize(&path).await.unwrap_or_else(|_| path.clone());
                        InputFile::file_id(format!("file://{}", abs_path.to_string_lossy()).into())
                    } else {
                        InputFile::file(path.clone())
                    };

                    let bot_watermark = crate::utils::BOT_WATERMARK;
                    if *is_audio {
                        let mut audio = InputMediaAudio::new(input_file)
                            .title(title.clone())
                            .caption(bot_watermark.trim_start().to_string());
                        if let Some(perf) = performer {
                            audio = audio.performer(perf.clone());
                        }
                        if let Some(thumb) = thumb_path {
                            let thumb_file = if config.is_local_api() {
                                let abs_thumb = tokio::fs::canonicalize(thumb).await.unwrap_or_else(|_| PathBuf::from(thumb));
                                InputFile::file_id(
                                    format!("file://{}", abs_thumb.to_string_lossy()).into(),
                                )
                            } else {
                                InputFile::file(thumb.clone())
                            };
                            audio = audio.thumbnail(thumb_file);
                        }
                        media_group.push(InputMedia::Audio(audio));
                    } else {
                        let mut video =
                            InputMediaVideo::new(input_file).caption(bot_watermark.to_string());
                        if let Some(thumb) = thumb_path {
                            let thumb_file = if config.is_local_api() {
                                let abs_thumb = tokio::fs::canonicalize(thumb).await.unwrap_or_else(|_| PathBuf::from(thumb));
                                InputFile::file_id(
                                    format!("file://{}", abs_thumb.to_string_lossy()).into(),
                                )
                            } else {
                                InputFile::file(thumb.clone())
                            };
                            video.thumbnail = Some(thumb_file);
                        }
                        media_group.push(InputMedia::Video(video));
                    }
                }

                if !media_group.is_empty() {
                    let mut req = bot.send_media_group(chat_id, media_group);
                    if let Some(reply_id) = res.reply_to_message_id {
                        req = req.reply_parameters(teloxide::types::ReplyParameters::new(
                            teloxide::types::MessageId(reply_id),
                        ));
                    }
                    match req.await {
                        Ok(messages) => {
                            for (msg, file_tuple) in messages.iter().zip(chunk.iter()) {
                                let cache_key = &file_tuple.6;
                                if let Some(key) = cache_key {
                                    let extracted_file_id = if let Some(m) = msg.video() {
                                        Some(m.file.id.clone())
                                    } else if let Some(m) = msg.audio() {
                                        Some(m.file.id.clone())
                                    } else {
                                        None
                                    };

                                    if let Some(file_id) = extracted_file_id {
                                        if let Ok(mut conn) = redis_pool.get().await {
                                            let _: () = redis::cmd("SETEX")
                                                .arg(key)
                                                .arg(30 * 24 * 3600)
                                                .arg(&file_id.0)
                                                .query_async(&mut conn)
                                                .await
                                                .unwrap_or(());
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to send media group: {}", e);
                            let _ = bot
                                .send_message(
                                    chat_id,
                                    format!(
                                        "❌ Ошибка отправки части плейлиста в Telegram API: {}",
                                        e
                                    ),
                                )
                                .await;
                        }
                    }

                    // cleanup
                    for (file_path, _, _, _, thumb_path, _, _) in chunk {
                        let _ = tokio::fs::remove_file(file_path).await;
                        if let Some(t) = thumb_path {
                            let _ = tokio::fs::remove_file(t).await;
                        }
                    }
                }

                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            }

            if failed_count > 0 {
                let msg = format!(
                    "⚠️ Не удалось скачать {} треков из-за ограничений:\n{}",
                    failed_count,
                    failed_items.join("\n")
                );
                let _ = bot.send_message(chat_id, msg).await;
            }

            if let Some(msg_id) = res.status_message_id {
                let mid = teloxide::types::MessageId(msg_id);
                let _ = bot.delete_message(chat_id, mid).await;
            }

            for (file_path, _, _, _, thumb_path, _, _) in files {
                let _ = tokio::fs::remove_file(&file_path).await;
                if let Some(thumb) = thumb_path {
                    let _ = tokio::fs::remove_file(thumb).await;
                }
            }
        }
        TaskStatus::V2Completed { mut outputs, failed_count, failed_items } => {
            tracing::info!("Received V2Completed for chat {} with {} outputs", chat_id, outputs.len());
            
            outputs.sort_by_key(|o| match o.role {
                fsocial_common::OutputRole::Primary => 0,
                fsocial_common::OutputRole::Secondary => 1,
                fsocial_common::OutputRole::Caption => 2,
                fsocial_common::OutputRole::Log => 3,
            });

            let http_client = reqwest::Client::new();
            let resolver = DefaultUriResolver { http_client };
            let mapper = TelegramPresentationMapper { config, resolver: &resolver };
            let delivery = TelegramDelivery { bot, chat_id, config, resolver: &resolver };

            let mut conn = redis_pool.get().await.expect("Redis pool");

            for (idx, output) in outputs.into_iter().enumerate() {
                let idempotency_key = format!("idempotency:{}:{}:{}", res.task_id, idx, output.cache_key.as_deref().unwrap_or(""));
                let was_set: bool = redis::cmd("SETNX")
                    .arg(&idempotency_key)
                    .arg(1)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(true);
                
                if !was_set {
                    tracing::info!("Output {} already sent. Skipping duplicate.", idempotency_key);
                    continue;
                }
                let _: redis::RedisResult<()> = redis::cmd("EXPIRE").arg(&idempotency_key).arg(86400).query_async(&mut conn).await;

                match mapper.map(&output, res.reply_to_message_id).await {
                    Ok(presentation) => {
                        match delivery.deliver(presentation).await {
                            Ok(_) => tracing::info!("Successfully sent output to chat {}", chat_id),
                            Err(e) => tracing::error!("Failed to send output to chat {}: {}", chat_id, e),
                        }
                    },
                    Err(e) => tracing::error!("Failed to map presentation: {}", e),
                }

                if output.cleanup == fsocial_common::CleanupStrategy::DeleteAfterDelivery {
                    if let fsocial_common::OutputPayload::Resource { uri } = &output.payload {
                        if let fsocial_common::OutputUri::LocalFile(path_str) = uri {
                            let _ = tokio::fs::remove_file(path_str).await;
                        }
                    }
                    
                    let thumb_uri = match &output.metadata {
                        fsocial_common::OutputMetadata::Video(m) => &m.thumb_uri,
                        fsocial_common::OutputMetadata::Audio(m) => &m.thumb_uri,
                        fsocial_common::OutputMetadata::Document(m) => &m.thumb_uri,
                        _ => &None,
                    };
                    if let Some(fsocial_common::OutputUri::LocalFile(thumb_str)) = thumb_uri {
                        let _ = tokio::fs::remove_file(thumb_str).await;
                    }
                }
            }

            if failed_count > 0 {
                let msg = format!("⚠️ Не удалось скачать {} элементов:\n{}", failed_count, failed_items.join("\n"));
                let _ = bot.send_message(chat_id, msg).await;
            }

            if let Some(msg_id) = res.status_message_id {
                let mid = teloxide::types::MessageId(msg_id);
                let _ = bot.delete_message(chat_id, mid).await;
            }
        }
        TaskStatus::Failed {
            ref error,
            retryable,
        } => {
            if let Some(msg_id) = res.status_message_id {
                let mid = teloxide::types::MessageId(msg_id);
                let err_msg = format!("❌ Ошибка: {}", error);
                if let Err(_) = crate::utils::edit_message_text_or_caption(&bot, chat_id, mid, err_msg).await {
                }
            }
            if !retryable {
                let payload = serde_json::to_vec(&res).unwrap();
                let _ = nats
                    .client
                    .publish(fsocial_common::subjects::DLQ.to_string(), payload.into())
                    .await;
                tracing::error!(
                    "Task {} failed permanently. Sent to DLQ. Error: {}",
                    res.task_id,
                    error
                );
            }
        }
        _ => {}
    }
}

async fn handle_progress(bot: &crate::MyBot, res: TaskResult, task_states: &crate::TaskStates) {
    let state = task_states.get(&res.task_id).await;
    let is_paused = state.as_deref() == Some("paused");
    let is_aborted = state.as_deref() == Some("aborted");
    if is_paused || is_aborted {
        return;
    }

    match res.status {
        TaskStatus::Progress {
            percent: _,
            status_text,
        } => {
            if let Some(msg_id) = res.status_message_id {
                let text = format!("⏳ {}", status_text);
                let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![vec![
                    teloxide::types::InlineKeyboardButton::callback(
                        "🛑 Отмена",
                        format!("abort|{}", res.task_id),
                    ),
                ]]);

                if let Err(_) = bot
                    .edit_message_text(
                        teloxide::types::ChatId(res.chat_id),
                        teloxide::types::MessageId(msg_id),
                        text.clone(),
                    )
                    .reply_markup(keyboard.clone())
                    .await
                {
                    let _ = bot
                        .edit_message_caption(
                            teloxide::types::ChatId(res.chat_id),
                            teloxide::types::MessageId(msg_id),
                        )
                        .caption(text)
                        .reply_markup(keyboard)
                        .await;
                }
            }
        }
        TaskStatus::PlaylistProgress {
            completed,
            total,
            status_text,
        } => {
            if let Some(msg_id) = res.status_message_id {
                let text = format!(
                    "⏳ Скачивание плейлиста: {}/{}\n{}",
                    completed, total, status_text
                );
                let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![vec![
                    teloxide::types::InlineKeyboardButton::callback(
                        "⏸ Отменить (Пауза)",
                        format!("pause|{}", res.task_id),
                    ),
                ]]);

                if let Err(_) = bot
                    .edit_message_text(
                        teloxide::types::ChatId(res.chat_id),
                        teloxide::types::MessageId(msg_id),
                        text.clone(),
                    )
                    .reply_markup(keyboard.clone())
                    .await
                {
                    let _ = bot
                        .edit_message_caption(
                            teloxide::types::ChatId(res.chat_id),
                            teloxide::types::MessageId(msg_id),
                        )
                        .caption(text)
                        .reply_markup(keyboard)
                        .await;
                }
            }
        }
        _ => {}
    }
}

pub enum ResolvedResource {
    LocalTempFile(std::path::PathBuf),
    DirectUrl(String),
}

pub trait UriResolver: Send + Sync {
    fn resolve(&self, uri: &fsocial_common::OutputUri) -> impl std::future::Future<Output = Result<ResolvedResource, Box<dyn std::error::Error + Send + Sync>>> + Send;
    fn download_to_temp(&self, url: &str) -> impl std::future::Future<Output = Result<std::path::PathBuf, Box<dyn std::error::Error + Send + Sync>>> + Send;
}

pub struct DefaultUriResolver {
    pub http_client: reqwest::Client,
}

impl UriResolver for DefaultUriResolver {
    async fn resolve(&self, uri: &fsocial_common::OutputUri) -> Result<ResolvedResource, Box<dyn std::error::Error + Send + Sync>> {
        use fsocial_common::OutputUri;
        match uri {
            OutputUri::LocalFile(path) => Ok(ResolvedResource::LocalTempFile(std::path::PathBuf::from(path))),
            OutputUri::RemoteHttp(url) => Ok(ResolvedResource::DirectUrl(url.clone())),
            OutputUri::S3(_) => Err("S3 not implemented".into()),
        }
    }

    async fn download_to_temp(&self, url: &str) -> Result<std::path::PathBuf, Box<dyn std::error::Error + Send + Sync>> {
        let resp = self.http_client.get(url).send().await?.error_for_status()?;
        let bytes = resp.bytes().await?;
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join(format!("{}.tmp", uuid::Uuid::new_v4()));
        tokio::fs::write(&file_path, bytes).await?;
        Ok(file_path)
    }
}

pub enum TelegramMessage {
    Video { input_file: teloxide::types::InputFile, caption: Option<String>, thumb: Option<teloxide::types::InputFile>, reply_to: Option<i32>, fallback_url: Option<String> },
    Audio { input_file: teloxide::types::InputFile, caption: Option<String>, thumb: Option<teloxide::types::InputFile>, title: Option<String>, performer: Option<String>, reply_to: Option<i32>, fallback_url: Option<String> },
    Photo { input_file: teloxide::types::InputFile, caption: Option<String>, reply_to: Option<i32>, fallback_url: Option<String> },
    Document { input_file: teloxide::types::InputFile, caption: Option<String>, thumb: Option<teloxide::types::InputFile>, reply_to: Option<i32>, fallback_url: Option<String> },
    Text { text: String, reply_to: Option<i32> },
}

pub trait PresentationMapper: Send + Sync {
    fn map<'a>(&'a self, output: &'a fsocial_common::Output, reply_to_message_id: Option<i32>) -> impl std::future::Future<Output = Result<TelegramMessage, Box<dyn std::error::Error + Send + Sync>>> + Send + 'a;
}

pub struct TelegramPresentationMapper<'a, R: UriResolver> {
    pub config: &'a fsocial_common::AppConfig,
    pub resolver: &'a R,
}

impl<'a, R: UriResolver> PresentationMapper for TelegramPresentationMapper<'a, R> {
    fn map<'b>(&'b self, output: &'b fsocial_common::Output, reply_to_message_id: Option<i32>) -> impl std::future::Future<Output = Result<TelegramMessage, Box<dyn std::error::Error + Send + Sync>>> + Send + 'b {
        async move {
            use fsocial_common::{OutputPayload, OutputMetadata};
        let bot_watermark = crate::utils::BOT_WATERMARK;

        match &output.payload {
            OutputPayload::Resource { uri } => {
                let resource = self.resolver.resolve(uri).await?;
                let (input_file, fallback_url) = match resource {
                    ResolvedResource::LocalTempFile(path) => {
                        let path_to_send = if self.config.is_local_api() {
                            let abs_path = tokio::fs::canonicalize(&path).await.unwrap_or_else(|_| path.clone());
                            teloxide::types::InputFile::file_id(format!("file://{}", abs_path.to_string_lossy()).into())
                        } else {
                            teloxide::types::InputFile::file(path)
                        };
                        (path_to_send, None)
                    },
                    ResolvedResource::DirectUrl(url) => {
                        (teloxide::types::InputFile::url(url.parse()?), Some(url))
                    }
                };

                let mut resolved_thumb = None;
                let thumb_uri_opt = match &output.metadata {
                    OutputMetadata::Video(meta) => meta.thumb_uri.as_ref(),
                    OutputMetadata::Audio(meta) => meta.thumb_uri.as_ref(),
                    _ => None,
                };
                
                if let Some(thumb_uri) = thumb_uri_opt {
                    let thumb_res = self.resolver.resolve(thumb_uri).await?;
                    if let ResolvedResource::LocalTempFile(path) = thumb_res {
                            let path_to_send = if self.config.is_local_api() {
                                let abs_path = tokio::fs::canonicalize(&path).await.unwrap_or_else(|_| path.clone());
                                teloxide::types::InputFile::file_id(format!("file://{}", abs_path.to_string_lossy()).into())
                            } else {
                                teloxide::types::InputFile::file(path)
                            };
                            resolved_thumb = Some(path_to_send);
                        }
                    }

                match &output.metadata {
                    OutputMetadata::Video(_) => Ok(TelegramMessage::Video {
                        input_file, caption: Some(bot_watermark.to_string()), thumb: resolved_thumb, reply_to: reply_to_message_id, fallback_url
                    }),
                    OutputMetadata::Audio(meta) => Ok(TelegramMessage::Audio {
                        input_file, caption: Some(bot_watermark.trim_start().to_string()), thumb: resolved_thumb, 
                        title: meta.title.clone(), performer: meta.performer.clone(), reply_to: reply_to_message_id, fallback_url
                    }),
                    OutputMetadata::Image(_) => Ok(TelegramMessage::Photo {
                        input_file, caption: Some(bot_watermark.to_string()), reply_to: reply_to_message_id, fallback_url
                    }),
                    OutputMetadata::Document(_) | OutputMetadata::None => Ok(TelegramMessage::Document {
                        input_file, caption: Some(bot_watermark.to_string()), thumb: resolved_thumb, reply_to: reply_to_message_id, fallback_url
                    }),
                }
            },
            OutputPayload::InlineText { text } => {
                Ok(TelegramMessage::Text { text: text.clone(), reply_to: reply_to_message_id })
            }
        }
        }
    }
}

pub trait DeliveryStrategy {
    async fn deliver(&self, presentation: TelegramMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

pub struct TelegramDelivery<'a, R: UriResolver> {
    pub bot: &'a crate::MyBot,
    pub chat_id: teloxide::types::ChatId,
    pub config: &'a fsocial_common::AppConfig,
    pub resolver: &'a R,
}

impl<'a, R: UriResolver> DeliveryStrategy for TelegramDelivery<'a, R> {
    async fn deliver(&self, presentation: TelegramMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut temp_paths_to_clean = vec![];

        match presentation {
            TelegramMessage::Video { input_file, caption, thumb, reply_to, fallback_url } => {
                let mut req = self.bot.send_video(self.chat_id, input_file);
                if let Some(c) = &caption { req = req.caption(c.clone()); }
                if let Some(t) = &thumb { req = req.thumbnail(t.clone()); }
                if let Some(r) = reply_to { req = req.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(r))); }
                
                if let Err(e) = req.await {
                    if let Some(url) = fallback_url {
                        tracing::warn!("Telegram rejected DirectUrl Video, falling back: {}", e);
                        let temp_path = self.resolver.download_to_temp(&url).await?;
                        temp_paths_to_clean.push(temp_path.clone());
                        let fallback_input = if self.config.is_local_api() { teloxide::types::InputFile::file_id(format!("file://{}", tokio::fs::canonicalize(&temp_path).await.unwrap_or_else(|_| temp_path.clone()).to_string_lossy()).into()) } else { teloxide::types::InputFile::file(temp_path) };
                        let mut req2 = self.bot.send_video(self.chat_id, fallback_input);
                        if let Some(c) = caption { req2 = req2.caption(c); }
                        if let Some(t) = thumb { req2 = req2.thumbnail(t); }
                        if let Some(r) = reply_to { req2 = req2.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(r))); }
                        req2.await?;
                    } else { return Err(e.into()); }
                }
            },
            TelegramMessage::Audio { input_file, caption, thumb, title, performer, reply_to, fallback_url } => {
                let mut req = self.bot.send_audio(self.chat_id, input_file);
                if let Some(c) = &caption { req = req.caption(c.clone()); }
                if let Some(t) = &thumb { req = req.thumbnail(t.clone()); }
                if let Some(ti) = &title { req = req.title(ti.clone()); }
                if let Some(p) = &performer { req = req.performer(p.clone()); }
                if let Some(r) = reply_to { req = req.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(r))); }
                
                if let Err(e) = req.await {
                    if let Some(url) = fallback_url {
                        tracing::warn!("Telegram rejected DirectUrl Audio, falling back: {}", e);
                        let temp_path = self.resolver.download_to_temp(&url).await?;
                        temp_paths_to_clean.push(temp_path.clone());
                        let fallback_input = if self.config.is_local_api() { teloxide::types::InputFile::file_id(format!("file://{}", tokio::fs::canonicalize(&temp_path).await.unwrap_or_else(|_| temp_path.clone()).to_string_lossy()).into()) } else { teloxide::types::InputFile::file(temp_path) };
                        let mut req2 = self.bot.send_audio(self.chat_id, fallback_input);
                        if let Some(c) = caption { req2 = req2.caption(c); }
                        if let Some(t) = thumb { req2 = req2.thumbnail(t); }
                        if let Some(ti) = title { req2 = req2.title(ti); }
                        if let Some(p) = performer { req2 = req2.performer(p); }
                        if let Some(r) = reply_to { req2 = req2.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(r))); }
                        req2.await?;
                    } else { return Err(e.into()); }
                }
            },
            TelegramMessage::Photo { input_file, caption, reply_to, fallback_url } => {
                let mut req = self.bot.send_photo(self.chat_id, input_file);
                if let Some(c) = &caption { req = req.caption(c.clone()); }
                if let Some(r) = reply_to { req = req.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(r))); }
                
                if let Err(e) = req.await {
                    if let Some(url) = fallback_url {
                        tracing::warn!("Telegram rejected DirectUrl Photo, falling back: {}", e);
                        let temp_path = self.resolver.download_to_temp(&url).await?;
                        temp_paths_to_clean.push(temp_path.clone());
                        let fallback_input = if self.config.is_local_api() { teloxide::types::InputFile::file_id(format!("file://{}", tokio::fs::canonicalize(&temp_path).await.unwrap_or_else(|_| temp_path.clone()).to_string_lossy()).into()) } else { teloxide::types::InputFile::file(temp_path) };
                        let mut req2 = self.bot.send_photo(self.chat_id, fallback_input);
                        if let Some(c) = caption { req2 = req2.caption(c); }
                        if let Some(r) = reply_to { req2 = req2.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(r))); }
                        req2.await?;
                    } else { return Err(e.into()); }
                }
            },
            TelegramMessage::Document { input_file, caption, thumb, reply_to, fallback_url } => {
                let mut req = self.bot.send_document(self.chat_id, input_file);
                if let Some(c) = &caption { req = req.caption(c.clone()); }
                if let Some(t) = &thumb { req = req.thumbnail(t.clone()); }
                if let Some(r) = reply_to { req = req.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(r))); }
                
                if let Err(e) = req.await {
                    if let Some(url) = fallback_url {
                        tracing::warn!("Telegram rejected DirectUrl Document, falling back: {}", e);
                        let temp_path = self.resolver.download_to_temp(&url).await?;
                        temp_paths_to_clean.push(temp_path.clone());
                        let fallback_input = if self.config.is_local_api() { teloxide::types::InputFile::file_id(format!("file://{}", tokio::fs::canonicalize(&temp_path).await.unwrap_or_else(|_| temp_path.clone()).to_string_lossy()).into()) } else { teloxide::types::InputFile::file(temp_path) };
                        let mut req2 = self.bot.send_document(self.chat_id, fallback_input);
                        if let Some(c) = caption { req2 = req2.caption(c); }
                        if let Some(t) = thumb { req2 = req2.thumbnail(t); }
                        if let Some(r) = reply_to { req2 = req2.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(r))); }
                        req2.await?;
                    } else { return Err(e.into()); }
                }
            },
            TelegramMessage::Text { text, reply_to } => {
                let mut req = self.bot.send_message(self.chat_id, text);
                if let Some(r) = reply_to { req = req.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(r))); }
                req.await?;
            }
        }

        for temp in temp_paths_to_clean {
            let _ = tokio::fs::remove_file(temp).await;
        }

        Ok(())
    }
}
