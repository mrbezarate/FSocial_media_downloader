use fsocial_common::{AppConfig, TaskResult, TaskStatus};
use futures::StreamExt;
use std::path::PathBuf;
use teloxide::{prelude::*, types::InputFile};
use tracing::{error, info};

use crate::nats_client::NatsClient;

pub async fn listen(bot: Bot, nats: NatsClient, config: AppConfig) {
    let mut results_sub = nats.subscribe_results().await.expect("Results sub failed");
    let mut progress_sub = nats.subscribe_progress().await.expect("Progress sub failed");

    loop {
        tokio::select! {
            Some(msg) = results_sub.next() => {
                if let Ok(res) = serde_json::from_slice::<TaskResult>(&msg.payload) {
                    handle_result(&bot, res, &config).await;
                }
            }
            Some(msg) = progress_sub.next() => {
                if let Ok(res) = serde_json::from_slice::<TaskResult>(&msg.payload) {
                    handle_progress(&bot, res).await;
                }
            }
        }
    }
}

async fn handle_result(bot: &Bot, res: TaskResult, config: &AppConfig) {
    let chat_id = teloxide::types::ChatId(res.chat_id);

    match res.status {
        TaskStatus::Completed {
            file_path,
            title,
            is_audio,
            performer,
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
                    return;
                }
            }
            let input_file = match tokio::fs::read(&path).await {
                Ok(data) => {
                    let file_name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
                    InputFile::memory(data).file_name(file_name)
                },
                Err(e) => {
                    tracing::error!("Failed to read file {} to memory: {}", path.display(), e);
                    InputFile::file(path.clone())
                }
            };

            let send_result = if is_audio {
                let mut req = bot.send_audio(chat_id, input_file).title(title);
                if let Some(perf) = performer {
                    req = req.performer(perf);
                }
                if let Some(reply_id) = res.reply_to_message_id {
                    req = req.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(reply_id)));
                }
                req.await
            } else {
                let mut req = bot.send_video(chat_id, input_file).caption(title);
                if let Some(reply_id) = res.reply_to_message_id {
                    req = req.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(reply_id)));
                }
                req.await
            };

            match send_result {
                Ok(_) => {
                    info!("Successfully sent file to chat {}", chat_id);
                    if let Some(msg_id) = res.status_message_id {
                        let mid = teloxide::types::MessageId(msg_id);
                        let _ = bot.delete_message(chat_id, mid).await;
                    }
                    if let Err(e) = tokio::fs::remove_file(&file_path).await {
                        error!("Failed to clean up file {}: {}", file_path, e);
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
        TaskStatus::Failed { error, .. } => {
            if let Some(msg_id) = res.status_message_id {
                let mid = teloxide::types::MessageId(msg_id);
                let err_msg = format!("❌ Ошибка: {}", error);
                if let Err(_) = bot.edit_message_text(chat_id, mid, err_msg.clone()).await {
                    let _ = bot.edit_message_caption(chat_id, mid).caption(err_msg).await;
                }
            }
        }
        _ => {}
    }
}

async fn handle_progress(bot: &Bot, res: TaskResult) {
    if let TaskStatus::Progress { percent, status_text } = res.status {
        if let Some(msg_id) = res.status_message_id {
            let filled = (percent / 10) as usize;
            let empty = 10 - filled;
            let bar = format!("{}{}", "🟩".repeat(filled), "⬜".repeat(empty));
            let text = format!("⏳ {}\n{} {}%", status_text, bar, percent);

            // We can't know if it's a photo or text easily here. 
            // So we try both! 
            if let Err(_) = bot.edit_message_text(teloxide::types::ChatId(res.chat_id), teloxide::types::MessageId(msg_id), text.clone()).await {
                let _ = bot.edit_message_caption(teloxide::types::ChatId(res.chat_id), teloxide::types::MessageId(msg_id)).caption(text).await;
            }
        }
    }
}
