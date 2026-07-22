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
    publish_result(js, &res).await;
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
                            Ok(fsocial_common::InfoResponse {
                                title: "Spotify Audio".to_string(),
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
                                is_playlist: req.url.contains("/playlist/") || req.url.contains("/album/"),
                                playlist_count: None,
                                playlist_urls: vec![],
                                error: None,
                            })
                        } else if req.url.contains("soundcloud.com") {
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
                                is_playlist: req.url.contains("/sets/"),
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

            let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(100);
            let ctx_prog = ctx_clone.clone();
            let task_id_prog = task.task_id.clone();
            let chat_id_prog = task.chat_id;
            let status_msg_id_prog = task.status_message_id;

            tokio::spawn(async move {
                let mut last_percent = 0;
                while let Some(line) = rx.recv().await {
                    if let Some(caps) = re.captures(&line) {
                        if let (Some(p), Some(size)) = (caps.name("percent"), caps.name("size")) {
                            if let Ok(percent) = p.as_str().parse::<f32>() {
                                let pct = percent as u8;
                                // Only update every 10% to avoid telegram rate limits
                                if pct >= last_percent + 10 || pct == 100 {
                                    last_percent = pct;
                                    let speed = caps.name("speed").map_or("?", |m| m.as_str());
                                    let eta = caps.name("eta").map_or("?", |m| m.as_str());
                                    let text = format!("Размер: {} | Скорость: {} | Осталось: {}", size.as_str(), speed, eta);
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
            });

            let result = if task.platform == Platform::Spotify {
                audio::process_spotify_task(&ctx_clone, &task, Some(tx)).await
            } else {
                media::process_media_task(&ctx_clone, &task, Some(tx)).await
                    .map(|(path, title, dur)| (path, title, dur, None))
            };

            match result {
                Ok((file_path, title, duration_secs, performer)) => {
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
                            thumb_path: None,
                            is_audio: task.quality.is_audio(),
                        },
                    };
                    publish_result(&ctx_clone.nats_jetstream, &res).await;
                    if let Err(e) = msg.ack().await {
                        error!("Failed to ack message: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("Task {} failed: {:?}", task.task_id, e);
                    let retryable = e.is_retryable();
                    let res = TaskResult {
                        task_id: task.task_id.clone(),
                        chat_id: task.chat_id,
                        status_message_id: task.status_message_id,
                        reply_to_message_id: task.reply_to_message_id,
                        is_group: task.is_group,
                        status: TaskStatus::Failed {
                            error: e.to_string(),
                            retryable,
                        },
                    };
                    publish_result(&ctx_clone.nats_jetstream, &res).await;

                    if retryable {
                        // Don't ack — JetStream will redeliver after ack timeout
                        tracing::warn!("Task {} failed with retryable error, will be redelivered", task.task_id);
                    } else {
                        // Ack non-retryable errors to prevent infinite redelivery
                        if let Err(e) = msg.ack().await {
                            error!("Failed to ack unretryable message: {:?}", e);
                        }
                    }
                }
            }
        });
    }
}
