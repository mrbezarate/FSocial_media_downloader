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
    #[allow(dead_code)]
    pub redis_pool: deadpool_redis::Pool,
    pub cache: media::cache::MetadataCache,
    pub proxy_pool: media::proxy::ProxyPool,
    pub task_states: moka::future::Cache<String, fsocial_common::TaskCommandAction>,
}

pub async fn publish_result(client: &async_nats::Client, result: &TaskResult) {
    let payload = serde_json::to_vec(result).unwrap();
    if let Err(e) = client.publish(subjects::TASK_RESULTS.to_string(), payload.into()).await {
        error!("Failed to publish TaskResult: {:?}", e);
    }
}

pub async fn publish_progress(
    client: &async_nats::Client,
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
        status_is_media: false,
        reply_to_message_id: None,
        is_group: false,
        status: TaskStatus::Progress {
            percent,
            status_text: text.to_string(),
        },
    };
    let payload = serde_json::to_vec(&res).unwrap();
    if let Err(e) = client.publish(fsocial_common::subjects::TASK_PROGRESS.to_string(), payload.into()).await {
        tracing::error!("Failed to publish TaskProgress: {:?}", e);
    }
}

pub async fn publish_playlist_progress(
    client: &async_nats::Client,
    task_id: &str,
    chat_id: i64,
    status_message_id: Option<i32>,
    completed: u32,
    total: u32,
    text: &str,
) {
    let res = TaskResult {
        task_id: task_id.to_string(),
        chat_id,
        status_message_id,
        status_is_media: false,
        reply_to_message_id: None,
        is_group: false,
        status: TaskStatus::PlaylistProgress {
            completed,
            total,
            status_text: text.to_string(),
        },
    };
    let payload = serde_json::to_vec(&res).unwrap();
    if let Err(e) = client.publish(fsocial_common::subjects::TASK_PROGRESS.to_string(), payload.into()).await {
        tracing::error!("Failed to publish TaskProgress: {:?}", e);
    }
}

pub async fn run(ctx: Arc<WorkerContext>, mut shutdown_rx: tokio::sync::watch::Receiver<bool>) {
    let stream = match ctx.nats_jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: subjects::STREAM_NAME.to_string(),
            subjects: vec![subjects::DOWNLOAD_TASKS_WILDCARD.to_string()],
            retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
            max_age: std::time::Duration::from_secs(7200),
            ..Default::default()
        })
        .await
    {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to create JetStream stream: {:?}", e);
            return;
        }
    };

    let premium_consumer = stream
        .get_or_create_consumer(
            "worker-group-premium",
            Config {
                durable_name: Some("worker-group-premium".to_string()),
                filter_subject: subjects::DOWNLOAD_TASKS_PREMIUM.to_string(),
                ack_wait: std::time::Duration::from_secs(3600),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to create premium consumer");

    let free_consumer = stream
        .get_or_create_consumer(
            "worker-group-free",
            Config {
                durable_name: Some("worker-group-free".to_string()),
                filter_subject: subjects::DOWNLOAD_TASKS_FREE.to_string(),
                ack_wait: std::time::Duration::from_secs(3600),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to create free consumer");

    let semaphore = Arc::new(Semaphore::new(ctx.config.max_concurrent_downloads));
    let mut premium_messages = premium_consumer.messages().await.expect("Failed to get messages");
    let mut free_messages = free_consumer.messages().await.expect("Failed to get messages");

    crate::info_handler::start_info_listener(ctx.clone()).await;

    let cancel_sub = ctx.nats_client.subscribe(fsocial_common::subjects::TASK_COMMANDS.to_string()).await;
    if let Ok(mut sub) = cancel_sub {
        let task_states_clone = ctx.task_states.clone();
        tokio::spawn(async move {
            info!("Listening for TaskCommands...");
            while let Some(msg) = sub.next().await {
                if let Ok(cmd) = serde_json::from_slice::<fsocial_common::TaskCommand>(&msg.payload) {
                    task_states_clone.insert(cmd.task_id.clone(), cmd.action).await;
                }
            }
        });
    }

    info!("Worker started, listening for tasks...");

    let progress_re = regex::Regex::new(r"\[download\]\s+(?P<percent>[\d\.]+)%\s+of\s+(?P<size>[^\s]+)(?:\s+at\s+(?P<speed>[^\s]+))?(?:\s+ETA\s+(?P<eta>[\d:]+))?").unwrap();

    loop {
        let permit = tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("Shutdown signal received. Stop taking new NATS tasks.");
                    break;
                }
                continue;
            }
            p = semaphore.clone().acquire_owned() => {
                match p {
                    Ok(p) => p,
                    Err(_) => break,
                }
            }
        };

        let msg_res_opt = tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("Shutdown signal received while waiting for message.");
                    break;
                }
                continue;
            }
            opt = premium_messages.next() => opt,
            opt = free_messages.next() => opt,
        };

        let msg_res = match msg_res_opt {
            Some(m) => m,
            None => break,
        };
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
        let re = progress_re.clone();
        let ctx_clone = ctx.clone();

        tokio::spawn(async move {
            let _permit = permit;
            info!("Processing task: {}", task.task_id);

            publish_progress(
                &ctx_clone.nats_client,
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
                                        
                                        if last_update.elapsed().as_secs_f32() >= 3.5 || pct == 100 {
                                            last_update = tokio::time::Instant::now();
                                            let eta = caps.name("eta").map_or("?", |m| m.as_str());
                                            
                                            let mut text = format!("Скачивание: {}%", pct);
                                            if let Some(formatted_eta) = format_eta(eta) {
                                                text = format!("{} | Осталось: {}", text, formatted_eta);
                                            }

                                            if is_playlist_mode_prog {
                                                text = format!("Скачивание плейлиста: {}/{}\n⏳ {}", current_idx + 1, total_tracks, text);
                                                publish_playlist_progress(
                                                    &ctx_prog.nats_client,
                                                    &task_id_prog,
                                                    chat_id_prog,
                                                    status_msg_id_prog,
                                                    (current_idx + 1) as u32,
                                                    total_tracks as u32,
                                                    &text
                                                ).await;
                                            } else {
                                                publish_progress(
                                                    &ctx_prog.nats_client,
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
                }
            });

            let mut urls_to_process = if let Some(ref urls) = task.playlist_urls {
                urls.clone()
            } else {
                vec![task.url.clone()]
            };

            // Expand Spotify playlists if not already expanded (e.g. from group chat direct download)
            if task.platform == Platform::Spotify && urls_to_process.len() == 1 {
                let url = &urls_to_process[0];
                if url.contains("/playlist/") || url.contains("/album/") {
                    if let Some((stype, sid)) = audio::spotify::SpotifyClient::parse_spotify_url(url) {
                        let spotify_client = audio::spotify::SpotifyClient::new();
                        let mut expanded = Vec::new();
                        if stype == audio::spotify::SpotifyType::Playlist {
                            if let Ok(u) = spotify_client.get_playlist_track_urls(&ctx_clone.config, &sid).await {
                                expanded = u;
                            }
                        } else if stype == audio::spotify::SpotifyType::Album {
                            if let Ok(u) = spotify_client.get_album_track_urls(&ctx_clone.config, &sid).await {
                                expanded = u;
                            }
                        }
                        if !expanded.is_empty() {
                            urls_to_process = expanded;
                        }
                    }
                }
            }
            let total = urls_to_process.len();
            let is_playlist_mode = total > 1;
            
            let mut completed_files = Vec::new();
            let mut failed_items = Vec::new();
            let mut first_error = None;

            use futures::StreamExt;
            let tx_for_stream = tx.clone();
            let task_for_stream = task.clone();
            let ctx_for_stream = ctx_clone.clone();
            
            let mut stream = futures::stream::iter(urls_to_process.into_iter().enumerate())
                .map(move |(idx, url)| {
                    let mut current_task = task_for_stream.clone();
                    current_task.url = url.clone();
                    current_task.spotify_meta = None;
                    let ctx_clone = ctx_for_stream.clone();
                    let tx_clone = tx_for_stream.clone();
                    
                    async move {
                        if is_playlist_mode {
                            let mut is_aborted = false;
                            loop {
                                let state = ctx_clone.task_states.get(&current_task.task_id).await;
                                match state {
                                    Some(fsocial_common::TaskCommandAction::Abort) => {
                                        is_aborted = true;
                                        break;
                                    }
                                    Some(fsocial_common::TaskCommandAction::Pause) => {
                                        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                                        continue;
                                    }
                                    _ => break,
                                }
                            }
                            if is_aborted {
                                return (idx, url.clone(), Err(fsocial_common::AppError::Download("Скачивание прервано пользователем".into())));
                            }
                            let _ = tx_clone.send(fsocial_common::ProgressEvent::NewTrack(idx, total)).await;
                        }

                        let mut attempts = 0;
                        let max_attempts = 3;
                        let mut final_err = None;

                        while attempts < max_attempts {
                            attempts += 1;
                            let result = if current_task.platform == Platform::Spotify {
                                crate::audio::process_spotify_task(&ctx_clone, &current_task, Some(tx_clone.clone())).await
                            } else {
                                crate::media::process_media_task(&ctx_clone, &current_task, Some(tx_clone.clone())).await
                            };

                            match result {
                                Ok(res) => {
                                    let path = std::path::PathBuf::from(&res.0);
                                    if let Ok(meta) = tokio::fs::metadata(&path).await {
                                        let size_mb = meta.len() as f64 / (1024.0 * 1024.0);
                                        let max_size = if ctx_clone.config.is_local_api() { 1024.0 } else { 50.0 };
                                        if size_mb > max_size {
                                            let _ = tokio::fs::remove_file(&path).await;
                                            if let Some(thumb) = &res.4 {
                                                let _ = tokio::fs::remove_file(thumb).await;
                                            }
                                            
                                            if let Some(lower_quality) = current_task.quality.downgrade() {
                                                tracing::info!("File too big ({:.1}MB), downgrading from {:?} to {:?}", size_mb, current_task.quality, lower_quality);
                                                let _ = tx_clone.send(fsocial_common::ProgressEvent::Line(format!("⚠️ Слишком большой файл ({:.1}MB). Пробуем качество ниже...", size_mb))).await;
                                                current_task.quality = lower_quality;
                                                attempts = 0;
                                                continue;
                                            } else {
                                                final_err = Some(fsocial_common::AppError::Download(format!("Файл слишком большой ({:.1} МБ). Понижение качества невозможно.", size_mb)));
                                                break;
                                            }
                                        }
                                    }
                                    return (idx, url.clone(), Ok((res.0, res.1, res.2, res.3, res.4, current_task.quality.clone())))
                                },
                                Err(e) => {
                                    if !e.is_retryable() || attempts >= max_attempts {
                                        final_err = Some(e);
                                        break;
                                    }
                                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                                }
                            }
                        }
                        (idx, url.clone(), Err(final_err.unwrap()))
                    }
                })
                .buffer_unordered(if is_playlist_mode { 3 } else { 1 });

            while let Some((_idx, url, res)) = stream.next().await {
                match res {
                    Ok((file_path, title, duration_secs, performer, thumb_path, final_quality)) => {
                        if !task.is_premium && task.user_id > 0 {
                            if let Ok(meta) = tokio::fs::metadata(&file_path).await {
                                let size = meta.len();
                                if let Ok(mut conn) = ctx_clone.redis_pool.get().await {
                                    let bytes_key = format!("today_bytes:{}", task.user_id);
                                    let dl_key = format!("today_downloads:{}", task.user_id);
                                    
                                    let _ : redis::RedisResult<()> = redis::pipe()
                                        .cmd("INCRBY").arg(&bytes_key).arg(size)
                                        .cmd("EXPIRE").arg(&bytes_key).arg(86400)
                                        .cmd("INCR").arg(&dl_key)
                                        .cmd("EXPIRE").arg(&dl_key).arg(86400)
                                        .query_async(&mut conn).await;
                                }
                            }
                        }
                        
                        let cache_key = Some(format!("file_id:{}:{}", final_quality.callback_id(), url));
                        completed_files.push((file_path, title, duration_secs, performer, thumb_path, final_quality.is_audio(), cache_key));
                    }
                    Err(e) => {
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                        failed_items.push(url);
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
                        status_is_media: task.status_is_media,
                        reply_to_message_id: task.reply_to_message_id,
                        is_group: task.is_group,
                        status: TaskStatus::Failed {
                            error: e.to_string(),
                            retryable: e.is_retryable(),
                        },
                    };
                    publish_result(&ctx_clone.nats_client, &res).await;
                    if !e.is_retryable() {
                        let _ = msg.ack().await;
                    } else {
                        let _ = msg.ack_with(async_nats::jetstream::AckKind::Nak(None)).await;
                    }
                } else {
                    let completed_files_len = completed_files.len();
                    let res = TaskResult {
                        task_id: task.task_id.clone(),
                        chat_id: task.chat_id,
                        status_message_id: task.status_message_id,
                        status_is_media: task.status_is_media,
                        reply_to_message_id: task.reply_to_message_id,
                        is_group: task.is_group,
                        status: TaskStatus::PlaylistCompleted {
                            files: completed_files,
                            playlist_title: "Плейлист".to_string(),
                            failed_count: (total - completed_files_len) as u32,
                            failed_items,
                        },
                    };
                    publish_result(&ctx_clone.nats_client, &res).await;
                    let _ = msg.ack().await;
                }
            } else {
                if let Some(e) = first_error {
                    let res = TaskResult {
                        task_id: task.task_id.clone(),
                        chat_id: task.chat_id,
                        status_message_id: task.status_message_id,
                        status_is_media: task.status_is_media,
                        reply_to_message_id: task.reply_to_message_id,
                        is_group: task.is_group,
                        status: TaskStatus::Failed {
                            error: e.to_string(),
                            retryable: e.is_retryable(),
                        },
                    };
                    publish_result(&ctx_clone.nats_client, &res).await;
                    if !e.is_retryable() {
                        let _ = msg.ack().await;
                    } else {
                        let _ = msg.ack_with(async_nats::jetstream::AckKind::Nak(None)).await;
                    }
                } else if !completed_files.is_empty() {
                    let (file_path, title, duration_secs, performer, thumb_path, is_audio, cache_key) = completed_files.remove(0);
                    let res = TaskResult {
                        task_id: task.task_id.clone(),
                        chat_id: task.chat_id,
                        status_message_id: task.status_message_id,
                        status_is_media: task.status_is_media,
                        reply_to_message_id: task.reply_to_message_id,
                        is_group: task.is_group,
                        status: TaskStatus::Completed {
                            file_path,
                            title,
                            duration_secs,
                            performer,
                            thumb_path,
                            is_audio,
                            cache_key,
                        },
                    };
                    publish_result(&ctx_clone.nats_client, &res).await;
                    let _ = msg.ack().await;
                }
            }
        });
    }
    
    info!("Waiting for ongoing tasks to finish...");
    let _ = semaphore.acquire_many(ctx.config.max_concurrent_downloads as u32).await.unwrap();
    info!("All tasks finished successfully.");
}
