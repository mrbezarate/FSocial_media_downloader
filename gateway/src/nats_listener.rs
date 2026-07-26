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
    let mut progress_sub = nats.subscribe_progress().await.expect("Progress sub failed");

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

async fn handle_result(bot: &crate::MyBot, res: TaskResult, config: &AppConfig, redis_pool: &deadpool_redis::Pool, nats: &crate::NatsClient) {
    let chat_id = teloxide::types::ChatId(res.chat_id);

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
            let path = PathBuf::from(&file_path);

            if let Ok(meta) = tokio::fs::metadata(&path).await {
                let size_mb = meta.len() as f64 / (1024.0 * 1024.0);
                if size_mb > 50.0 && !config.is_local_api() {
                    if let Some(msg_id) = res.status_message_id {
                        let _ = bot.delete_message(chat_id, teloxide::types::MessageId(msg_id)).await;
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
            let _file_name = path.file_name().unwrap_or_default().to_string_lossy().to_string();

            let mut edit_success = false;
            let mut edit_res_msg = None;

            if res.status_is_media {
                if let Some(msg_id) = res.status_message_id {
                    let mid = teloxide::types::MessageId(msg_id);
                    let input_file = if config.is_local_api() {
                        let abs_path = std::fs::canonicalize(&path).unwrap_or(path.clone());
                        InputFile::file_id(format!("file://{}", abs_path.to_string_lossy()).into())
                    } else {
                        InputFile::file(path.clone())
                    };

                    let bot_watermark = "\n\nСкачано с помощью бота @FSocial_Media_Downloader_bot";
                    let media = if is_audio {
                        let mut aud = teloxide::types::InputMediaAudio::new(input_file).title(title.clone()).caption(bot_watermark.trim_start().to_string());
                        if let Some(perf) = &performer {
                            aud.performer = Some(perf.clone());
                        }
                        if let Some(thumb) = &thumb_path {
                            let thumb_file = if config.is_local_api() {
                                let abs_thumb = std::fs::canonicalize(thumb).unwrap_or_else(|_| PathBuf::from(thumb));
                                InputFile::file_id(format!("file://{}", abs_thumb.to_string_lossy()).into())
                            } else {
                                InputFile::file(thumb.clone())
                            };
                            aud.thumbnail = Some(thumb_file);
                        }
                        teloxide::types::InputMedia::Audio(aud)
                    } else {
                        let path_ext = path.extension().unwrap_or_default().to_string_lossy().to_lowercase();
                        if path_ext == "gif" {
                            teloxide::types::InputMedia::Animation(teloxide::types::InputMediaAnimation::new(input_file).caption(bot_watermark.to_string()))
                        } else if path_ext == "jpg" || path_ext == "jpeg" || path_ext == "png" || path_ext == "webp" {
                            teloxide::types::InputMedia::Photo(teloxide::types::InputMediaPhoto::new(input_file).caption(bot_watermark.to_string()))
                        } else {
                            let mut vid = teloxide::types::InputMediaVideo::new(input_file).caption(bot_watermark.to_string());
                            if let Some(thumb) = &thumb_path {
                                let thumb_file = if config.is_local_api() {
                                    let abs_thumb = std::fs::canonicalize(thumb).unwrap_or_else(|_| PathBuf::from(thumb));
                                    InputFile::file_id(format!("file://{}", abs_thumb.to_string_lossy()).into())
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
                    let abs_path = std::fs::canonicalize(&path).unwrap_or(path.clone());
                    InputFile::file_id(format!("file://{}", abs_path.to_string_lossy()).into())
                } else {
                    InputFile::file(path.clone())
                };

                let bot_watermark = "\n\nСкачано с помощью бота @FSocial_Media_Downloader_bot";
                let api_res = if is_audio {
                    let mut req = bot.send_audio(chat_id, input_file).title(title.clone()).caption(bot_watermark.trim_start().to_string());
                    if let Some(perf) = &performer {
                        req = req.performer(perf.clone());
                    }
                    if let Some(thumb) = &thumb_path {
                        let thumb_file = if config.is_local_api() {
                            let abs_thumb = std::fs::canonicalize(thumb).unwrap_or_else(|_| PathBuf::from(thumb));
                            InputFile::file_id(format!("file://{}", abs_thumb.to_string_lossy()).into())
                        } else {
                            InputFile::file(thumb.clone())
                        };
                        req = req.thumbnail(thumb_file);
                    }
                    if let Some(reply_id) = res.reply_to_message_id {
                        req = req.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(reply_id)));
                    }
                    req.await
                } else {
                    let path_ext = path.extension().unwrap_or_default().to_string_lossy().to_lowercase();
                    if path_ext == "jpg" || path_ext == "jpeg" || path_ext == "png" || path_ext == "webp" {
                        let mut req = bot.send_photo(chat_id, input_file).caption(bot_watermark.to_string());
                        if let Some(reply_id) = res.reply_to_message_id {
                            req = req.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(reply_id)));
                        }
                        req.await
                    } else if path_ext == "gif" {
                        let mut req = bot.send_animation(chat_id, input_file).caption(bot_watermark.to_string());
                        if let Some(reply_id) = res.reply_to_message_id {
                            req = req.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(reply_id)));
                        }
                        req.await
                    } else {
                        let mut req = bot.send_video(chat_id, input_file).caption(bot_watermark.to_string());
                        if let Some(thumb) = &thumb_path {
                            let thumb_file = if config.is_local_api() {
                                let abs_thumb = std::fs::canonicalize(thumb).unwrap_or_else(|_| PathBuf::from(thumb));
                                InputFile::file_id(format!("file://{}", abs_thumb.to_string_lossy()).into())
                            } else {
                                InputFile::file(thumb.clone())
                            };
                            req = req.thumbnail(thumb_file);
                        }
                        if let Some(reply_id) = res.reply_to_message_id {
                            req = req.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(reply_id)));
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
                        let extracted_file_id = if let Some(m) = send_result.as_ref().unwrap().video() {
                            Some(m.file.id.clone())
                        } else if let Some(m) = send_result.as_ref().unwrap().audio() {
                            Some(m.file.id.clone())
                        } else if let Some(m) = send_result.as_ref().unwrap().photo().and_then(|p| p.last()) {
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
                        if let Err(_) = bot.edit_message_text(chat_id, mid, err_msg.clone()).await {
                            let _ = bot.edit_message_caption(chat_id, mid).caption(err_msg).await;
                        }
                    }
                }
            }
        }
        TaskStatus::PlaylistCompleted { files, playlist_title: _, failed_count, failed_items } => {
            use teloxide::types::{InputMedia, InputMediaAudio, InputMediaVideo};
            
            // Chunk files into groups of 10
            for chunk in files.chunks(10) {
                let mut media_group = Vec::new();
                for (file_path, title, _duration_secs, performer, thumb_path, is_audio, _cache_key) in chunk {
                    let path = PathBuf::from(file_path);
                    let input_file = if config.is_local_api() {
                        let abs_path = std::fs::canonicalize(&path).unwrap_or(path.clone());
                        InputFile::file_id(format!("file://{}", abs_path.to_string_lossy()).into())
                    } else {
                        InputFile::file(path.clone())
                    };

                    let bot_watermark = "\n\nСкачано с помощью бота @FSocial_Media_Downloader_bot";
                    if *is_audio {
                        let mut audio = InputMediaAudio::new(input_file).title(title.clone()).caption(bot_watermark.trim_start().to_string());
                        if let Some(perf) = performer {
                            audio = audio.performer(perf.clone());
                        }
                        if let Some(thumb) = thumb_path {
                            let thumb_file = if config.is_local_api() {
                                let abs_thumb = std::fs::canonicalize(thumb).unwrap_or_else(|_| PathBuf::from(thumb));
                                InputFile::file_id(format!("file://{}", abs_thumb.to_string_lossy()).into())
                            } else {
                                InputFile::file(thumb.clone())
                            };
                            audio = audio.thumbnail(thumb_file);
                        }
                        media_group.push(InputMedia::Audio(audio));
                    } else {
                        let mut video = InputMediaVideo::new(input_file).caption(bot_watermark.to_string());
                        if let Some(thumb) = thumb_path {
                            let thumb_file = if config.is_local_api() {
                                let abs_thumb = std::fs::canonicalize(thumb).unwrap_or_else(|_| PathBuf::from(thumb));
                                InputFile::file_id(format!("file://{}", abs_thumb.to_string_lossy()).into())
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
                        req = req.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(reply_id)));
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
                let msg = format!("⚠️ Не удалось скачать {} треков из-за ограничений:\n{}", failed_count, failed_items.join("\n"));
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
        TaskStatus::Failed { ref error, retryable } => {
            if let Some(msg_id) = res.status_message_id {
                let mid = teloxide::types::MessageId(msg_id);
                let err_msg = format!("❌ Ошибка: {}", error);
                if let Err(_) = bot.edit_message_text(chat_id, mid, err_msg.clone()).await {
                    let _ = bot.edit_message_caption(chat_id, mid).caption(err_msg).await;
                }
            }
            if !retryable {
                let payload = serde_json::to_vec(&res).unwrap();
                let _ = nats.client.publish(fsocial_common::subjects::DLQ.to_string(), payload.into()).await;
                tracing::error!("Task {} failed permanently. Sent to DLQ. Error: {}", res.task_id, error);
            }
        }
        _ => {}
    }
}

async fn handle_progress(bot: &crate::MyBot, res: TaskResult, task_states: &crate::TaskStates) {
    let state = task_states.get(&res.task_id).await;
    let is_paused = state.as_deref() == Some("paused");
    let is_aborted = state.as_deref() == Some("aborted");
    if is_paused || is_aborted { return; }

    match res.status {
        TaskStatus::Progress { percent: _, status_text } => {
            if let Some(msg_id) = res.status_message_id {
                let text = format!("⏳ {}", status_text);
                let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![vec![
                    teloxide::types::InlineKeyboardButton::callback("🛑 Отмена", format!("abort|{}", res.task_id))
                ]]);

                if let Err(_) = bot.edit_message_text(teloxide::types::ChatId(res.chat_id), teloxide::types::MessageId(msg_id), text.clone()).reply_markup(keyboard.clone()).await {
                    let _ = bot.edit_message_caption(teloxide::types::ChatId(res.chat_id), teloxide::types::MessageId(msg_id)).caption(text).reply_markup(keyboard).await;
                }
            }
        },
        TaskStatus::PlaylistProgress { completed, total, status_text } => {
            if let Some(msg_id) = res.status_message_id {
                let text = format!("⏳ Скачивание плейлиста: {}/{}\n{}", completed, total, status_text);
                let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![vec![
                    teloxide::types::InlineKeyboardButton::callback("⏸ Отменить (Пауза)", format!("pause|{}", res.task_id))
                ]]);
                
                if let Err(_) = bot.edit_message_text(teloxide::types::ChatId(res.chat_id), teloxide::types::MessageId(msg_id), text.clone()).reply_markup(keyboard.clone()).await {
                    let _ = bot.edit_message_caption(teloxide::types::ChatId(res.chat_id), teloxide::types::MessageId(msg_id)).caption(text).reply_markup(keyboard).await;
                }
            }
        },
        _ => {}
    }
}
