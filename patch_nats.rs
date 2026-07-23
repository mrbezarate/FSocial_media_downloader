            let file_data_opt = tokio::fs::read(&path).await.ok();
            let file_name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();

            let mut edit_success = false;

            if res.status_is_media {
                if let Some(msg_id) = res.status_message_id {
                    let mid = teloxide::types::MessageId(msg_id);
                    let input_file = match &file_data_opt {
                        Some(d) => InputFile::memory(d.clone()).file_name(file_name.clone()),
                        None => InputFile::file(path.clone())
                    };

                    let media = if *is_audio {
                        let mut aud = teloxide::types::InputMediaAudio::new(input_file).title(title.clone());
                        if let Some(perf) = performer {
                            aud.performer = Some(perf.clone());
                        }
                        if let Some(thumb) = thumb_path {
                            let thumb_file = match tokio::fs::read(thumb).await {
                                Ok(data) => InputFile::memory(data).file_name("cover.jpg"),
                                Err(_) => InputFile::file(thumb.clone())
                            };
                            aud.thumbnail = Some(thumb_file);
                        }
                        teloxide::types::InputMedia::Audio(aud)
                    } else {
                        let path_ext = path.extension().unwrap_or_default().to_string_lossy().to_lowercase();
                        if path_ext == "gif" {
                            teloxide::types::InputMedia::Animation(teloxide::types::InputMediaAnimation::new(input_file).caption(title.clone()))
                        } else if path_ext == "jpg" || path_ext == "jpeg" || path_ext == "png" || path_ext == "webp" {
                            teloxide::types::InputMedia::Photo(teloxide::types::InputMediaPhoto::new(input_file).caption(title.clone()))
                        } else {
                            teloxide::types::InputMedia::Video(teloxide::types::InputMediaVideo::new(input_file).caption(title.clone()))
                        }
                    };

                    if let Ok(_) = bot.edit_message_media(chat_id, mid, media).await {
                        edit_success = true;
                    }
                }
            }

            let send_result = if edit_success {
                Ok(())
            } else {
                let input_file = match file_data_opt {
                    Some(data) => InputFile::memory(data).file_name(file_name),
                    None => InputFile::file(path.clone())
                };

                let api_res = if *is_audio {
                    let mut req = bot.send_audio(chat_id, input_file).title(title);
                    if let Some(perf) = performer {
                        req = req.performer(perf.clone());
                    }
                    if let Some(thumb) = thumb_path {
                        let thumb_file = match tokio::fs::read(thumb).await {
                            Ok(data) => InputFile::memory(data).file_name("cover.jpg"),
                            Err(_) => InputFile::file(thumb.clone())
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
                        let mut req = bot.send_photo(chat_id, input_file).caption(title);
                        if let Some(reply_id) = res.reply_to_message_id {
                            req = req.reply_parameters(teloxide::types::ReplyParameters::new(teloxide::types::MessageId(reply_id)));
                        }
                        req.await
                    } else if path_ext == "gif" {
                        let mut req = bot.send_animation(chat_id, input_file).caption(title);
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
                    }
                };
                api_res.map(|_| ())
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
                    if let Some(thumb) = thumb_path {
                        let _ = tokio::fs::remove_file(thumb).await;
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
