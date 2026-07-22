use crate::{audio, media};
use async_nats::jetstream::{self, consumer::pull::Config};
use fsocial_common::{AppConfig, DownloadTask, Platform, TaskResult, TaskStatus};
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};
use fsocial_common::subjects;

pub struct WorkerContext {
    pub config: AppConfig,
    pub nats_client: async_nats::Client,
    pub nats_jetstream: jetstream::Context,
    pub redis_pool: deadpool_redis::Pool,
    pub cache: media::cache::MetadataCache,
    pub proxy_pool: media::proxy::ProxyPool,
}

pub async fn publish_result(js: &jetstream::Context, result: &TaskResult) {
    let payload = serde_json::to_vec(result).unwrap();
    if let Err(e) = js.publish(subjects::TASK_RESULTS.to_string(), payload.into()).await {
        error!("Failed to publish TaskResult: {:?}", e);
    }
}

pub async fn publish_progress(
    js: &jetstream::Context,
    task_id: &str,
    chat_id: i64,
    status_message_id: Option<i32>,
    percent: u8,
    text: &str,
) {
    let res = TaskResult {
        task_id: task_id.to_string(),
        chat_id,
        status_message_id,
        reply_to_message_id: None,
        is_group: false,
        status: TaskStatus::Progress {
            percent,
            status_text: text.to_string(),
        },
    };
    let payload = serde_json::to_vec(&res).unwrap();
    if let Err(e) = js.publish(fsocial_common::subjects::TASK_PROGRESS.to_string(), payload.into()).await {
        tracing::error!("Failed to publish TaskProgress: {:?}", e);
    }
}

pub async fn run(ctx: Arc<WorkerContext>) {
    let consumer = match ctx.nats_jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: subjects::STREAM_NAME.to_string(),
            subjects: vec![subjects::DOWNLOAD_TASKS.to_string()],
            ..Default::default()
        })
        .await
    {
        Ok(s) => match s
            .get_or_create_consumer(
                subjects::WORKER_GROUP,
                Config {
                    durable_name: Some(subjects::WORKER_GROUP.to_string()),
                    filter_subject: subjects::DOWNLOAD_TASKS.to_string(),
                    ack_wait: std::time::Duration::from_secs(3600),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to create JetStream consumer: {:?}", e);
                return;
            }
        },
        Err(e) => {
            error!("Failed to create JetStream stream: {:?}", e);
            return;
        }
    };

    let semaphore = Arc::new(Semaphore::new(ctx.config.max_concurrent_downloads));
    let mut messages = consumer.messages().await.expect("Failed to get messages");

    let info_sub = ctx.nats_client.subscribe(subjects::INFO_REQUEST.to_string()).await;
    if let Ok(mut sub) = info_sub {
        let ctx_info = ctx.clone();
        tokio::spawn(async move {
            info!("Listening for InfoRequests...");
            while let Some(msg) = sub.next().await {
                if let Ok(req) = serde_json::from_slice::<fsocial_common::InfoRequest>(&msg.payload) {
                    if let Some(reply) = msg.reply {
                        let res = if req.url.contains("spotify.com") {
                            let mut playlist_urls = Vec::new();
                            let is_playlist = req.url.contains("/playlist/") || req.url.contains("/album/");
                            
                            if is_playlist {
                                let client = reqwest::Client::new();
                                if let Ok(resp) = client.get(&req.url).send().await {
                                    if let Ok(html) = resp.text().await {
                                        let re = regex::Regex::new(r"spotify:track:([a-zA-Z0-9]+)").unwrap();
                                        for cap in re.captures_iter(&html) {
                                            if let Some(id) = cap.get(1) {
                                                let track_url = format!("https://open.spotify.com/track/{}", id.as_str());
                                                if !playlist_urls.contains(&track_url) {
                                                    playlist_urls.push(track_url);
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            Ok(fsocial_common::InfoResponse {
                                title: if is_playlist { "Spotify Плейлист/Альбом".to_string() } else { "Spotify Audio".to_string() },
                                uploader: Some("Spotify".to_string()),
                                thumbnail: None,
                                duration_secs: None,
                                available_qualities: vec![fsocial_common::QualityOption {
                                    quality: fsocial_common::Quality::AudioBest,
                                    filesize_bytes: None,
                                    estimated_secs: None,
                                    speed_category: "🚀".to_string(),
                                    display_label: "🎵 MP3".to_string(),
                                    full_button_label: "🎵 MP3".to_string(),
                                }],
                                is_playlist,
                                playlist_count: if is_playlist { Some(playlist_urls.len() as u32) } else { None },
                                playlist_urls,
                                error: None,
                            })
                        } else if req.url.contains("soundcloud.com") {
                            let is_sc_playlist = req.url.split('?').next().unwrap_or(&req.url).contains("/sets/");
                            Ok(fsocial_common::InfoResponse {
                                title: "SoundCloud Audio".to_string(),
                                uploader: Some("SoundCloud".to_string()),
                                thumbnail: None,
                                duration_secs: None,
                                available_qualities: vec![fsocial_common::QualityOption {
                                    quality: fsocial_common::Quality::AudioBest,
                                    filesize_bytes: None,
                                    estimated_secs: None,
                                    speed_category: "🚀".to_string(),
                                    display_label: "🎵 MP3".to_string(),
                                    full_button_label: "🎵 MP3".to_string(),
                                }],
                                is_playlist: is_sc_playlist,
                                playlist_count: None,
                                playlist_urls: vec![],
                                error: None,
                            })
                        } else {
                            let proxy = ctx_info.proxy_pool.next();
                            media::ytdlp::get_info(&ctx_info.config, &req.url, proxy).await
                        };
                        let reply_data = match res {
                            Ok(info_res) => info_res,
                            Err(e) => fsocial_common::InfoResponse {
                                title: String::new(),
                                uploader: None,
                                thumbnail: None,
                                duration_secs: None,
                                available_qualities: vec![],
                                is_playlist: false,
                                playlist_count: None,
                                playlist_urls: vec![],
                                error: Some(e.to_string()),
                            }
                        };
                        let payload = serde_json::to_vec(&reply_data).unwrap();
                        let _ = ctx_info.nats_client.publish(reply, payload.into()).await;
                    }
                }
            }
        });
    }

    info!("Worker started, listening for tasks...");

    let progress_re = regex::Regex::new(r"\[download\]\s+(?P<percent>[\d\.]+)%\s+of\s+(?P<size>[^\s]+)(?:\s+at\s+(?P<speed>[^\s]+))?(?:\s+ETA\s+(?P<eta>[\d:]+))?").unwrap();

    while let Some(msg_res) = messages.next().await {
        let msg = match msg_res {
            Ok(m) => m,
            Err(e) => {
                error!("Error receiving NATS message: {:?}", e);
                continue;
            }
        };

        let task: DownloadTask = match serde_json::from_slice(&msg.payload) {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to parse DownloadTask: {:?}", e);
                let _ = msg.ack().await;
                continue;
            }
        };

        let ctx_clone = ctx.clone();
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => break,
        };
        let re = progress_re.clone();

        tokio::spawn(async move {
            let _permit = permit;
            info!("Processing task: {}", task.task_id);

            publish_progress(
                &ctx_clone.nats_jetstream,
                &task.task_id,
                task.chat_id,
                task.status_message_id,
                0,
                "Начинаю загрузку..."
            ).await;

            let (tx, mut rx) = tokio::sync::mpsc::channel::<fsocial_common::ProgressEvent>(100);
            let ctx_prog = ctx_clone.clone();
            let task_id_prog = task.task_id.clone();
            let chat_id_prog = task.chat_id;
            let status_msg_id_prog = task.status_message_id;
            let is_playlist_mode_prog = task.playlist_urls.as_ref().map_or(1, |v| v.len()) > 1;

            fn format_eta(eta: &str) -> Option<String> {
                let cleaned = eta.replace("~", "").replace("?", "");
                if cleaned.is_empty() { return None; }
                let parts: Vec<&str> = cleaned.split(':').collect();
                let mut result = String::new();
                if parts.len() == 3 {
                    let h = parts[0].parse::<u32>().unwrap_or(0);
                    let m = parts[1].parse::<u32>().unwrap_or(0);
                    let s = parts[2].parse::<u32>().unwrap_or(0);
                    if h > 0 { result.push_str(&format!("{} ч ", h)); }
                    if m > 0 { result.push_str(&format!("{} мин ", m)); }
                    if s > 0 || (h == 0 && m == 0) { result.push_str(&format!("{} сек", s)); }
                } else if parts.len() == 2 {
                    let m = parts[0].parse::<u32>().unwrap_or(0);
                    let s = parts[1].parse::<u32>().unwrap_or(0);
                    if m > 0 { result.push_str(&format!("{} мин ", m)); }
                    if s > 0 || m == 0 { result.push_str(&format!("{} сек", s)); }
                } else { return None; }
                Some(result.trim().to_string())
            }

            tokio::spawn(async move {
                let mut last_update = tokio::time::Instant::now() - tokio::time::Duration::from_secs(10);
                let mut current_idx = 0;
                let mut total_tracks = 1;

                while let Some(event) = rx.recv().await {
                    match event {
                        fsocial_common::ProgressEvent::NewTrack(idx, total) => {
                            current_idx = idx;
                            total_tracks = total;
                        }
                        fsocial_common::ProgressEvent::Line(line) => {
                            if let Some(caps) = re.captures(&line) {
                                if let (Some(p), Some(_size)) = (caps.name("percent"), caps.name("size")) {
                                    if let Ok(percent) = p.as_str().parse::<f32>() {
                                        let pct = percent as u8;
                                        
                                        if last_update.elapsed().as_secs_f32() >= 1.5 || pct == 100 {
                                            last_update = tokio::time::Instant::now();
                                            let eta = caps.name("eta").map_or("?", |m| m.as_str());
                                            
                                            let mut text = format!("Скачивание: {}%", pct);
                                            if let Some(formatted_eta) = format_eta(eta) {
                                                text = format!("{} | Осталось: {}", text, formatted_eta);
                                            }

                                            if is_playlist_mode_prog {
                                                text = format!("Скачивание плейлиста: {}/{}\n⏳ {}", current_idx + 1, total_tracks, text);
                                            }

                                            publish_progress(
                                                &ctx_prog.nats_jetstream,
                                                &task_id_prog,
                                                chat_id_prog,
                                                status_msg_id_prog,
                                                pct,
                                                &text
                                            ).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });

            let urls_to_process = if let Some(ref urls) = task.playlist_urls {
                urls.clone()
            } else {
                vec![task.url.clone()]
            };
            let total = urls_to_process.len();
            let is_playlist_mode = total > 1;
            
            let mut completed_files = Vec::new();
            let mut first_error = None;

            for (idx, url) in urls_to_process.iter().enumerate() {
                if is_playlist_mode {
                    let _ = tx.send(fsocial_common::ProgressEvent::NewTrack(idx, total)).await;
                }
                
                // ack_wait is set to 3600s so it won't redeliver

                let mut current_task = task.clone();
                current_task.url = url.clone();
                // Clear metadata so spotify processes this single track correctly
                current_task.spotify_meta = None;

                let result = if current_task.platform == Platform::Spotify {
                    audio::process_spotify_task(&ctx_clone, &current_task, Some(tx.clone())).await
                } else {
                    media::process_media_task(&ctx_clone, &current_task, Some(tx.clone())).await
                        .map(|(path, title, dur)| (path, title, dur, None, None))
                };

                match result {
                    Ok((file_path, title, duration_secs, performer, thumb_path)) => {
                        completed_files.push((file_path, title, duration_secs, performer, thumb_path, current_task.quality.is_audio()));
                    }
                    Err(e) => {
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                        tracing::warn!("Task {} failed on url {}: {:?}", task.task_id, url, first_error);
                        if !is_playlist_mode {
                            break;
                        }
                    }
                }
            }

            drop(tx); // drop the original tx so the rx loop finishes

            if is_playlist_mode {
                if completed_files.is_empty() {
                    let e = first_error.unwrap_or(fsocial_common::AppError::Download("Плейлист пуст или ошибка скачивания".into()));
                    let res = TaskResult {
                        task_id: task.task_id.clone(),
                        chat_id: task.chat_id,
                        status_message_id: task.status_message_id,
                        reply_to_message_id: task.reply_to_message_id,
                        is_group: task.is_group,
                        status: TaskStatus::Failed {
                            error: e.to_string(),
                            retryable: e.is_retryable(),
                        },
                    };
                    publish_result(&ctx_clone.nats_jetstream, &res).await;
                    if !e.is_retryable() {
                        let _ = msg.ack().await;
                    }
                } else {
                    let res = TaskResult {
                        task_id: task.task_id.clone(),
                        chat_id: task.chat_id,
                        status_message_id: task.status_message_id,
                        reply_to_message_id: task.reply_to_message_id,
                        is_group: task.is_group,
                        status: TaskStatus::PlaylistCompleted {
                            files: completed_files,
                            playlist_title: "Плейлист".to_string(),
                        },
                    };
                    publish_result(&ctx_clone.nats_jetstream, &res).await;
                    let _ = msg.ack().await;
                }
            } else {
                if let Some(e) = first_error {
                    let res = TaskResult {
                        task_id: task.task_id.clone(),
                        chat_id: task.chat_id,
                        status_message_id: task.status_message_id,
                        reply_to_message_id: task.reply_to_message_id,
                        is_group: task.is_group,
                        status: TaskStatus::Failed {
                            error: e.to_string(),
                            retryable: e.is_retryable(),
                        },
                    };
                    publish_result(&ctx_clone.nats_jetstream, &res).await;
                    if !e.is_retryable() {
                        let _ = msg.ack().await;
                    }
                } else if !completed_files.is_empty() {
                    let (file_path, title, duration_secs, performer, thumb_path, is_audio) = completed_files.remove(0);
                    let res = TaskResult {
                        task_id: task.task_id.clone(),
                        chat_id: task.chat_id,
                        status_message_id: task.status_message_id,
                        reply_to_message_id: task.reply_to_message_id,
                        is_group: task.is_group,
                        status: TaskStatus::Completed {
                            file_path,
                            title,
                            duration_secs,
                            performer,
                            thumb_path,
                            is_audio,
                        },
                    };
                    publish_result(&ctx_clone.nats_jetstream, &res).await;
                    let _ = msg.ack().await;
                }
            }
        });
    }
}
