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

pub async fn run(ctx: Arc<WorkerContext>) {
    let consumer = match ctx.nats_jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: subjects::STREAM_NAME.to_string(),
            subjects: vec![subjects::DOWNLOAD_TASKS.to_string()],
            retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
            max_age: std::time::Duration::from_secs(7200),
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
                            let mut title = if is_playlist { "Spotify Плейлист/Альбом".to_string() } else { "Spotify Audio".to_string() };
                            let mut thumbnail = None;
                            let mut uploader = Some("Spotify".to_string());
                            let mut duration_secs = None;

                            let spotify_client = crate::audio::spotify::SpotifyClient::new();
                            if let Some((stype, sid)) = crate::audio::spotify::SpotifyClient::parse_spotify_url(&req.url) {
                                if stype == crate::audio::spotify::SpotifyType::Track {
                                    if let Ok(meta) = spotify_client.get_track(&ctx_info.config, &sid).await {
                                        title = meta.title.clone();
                                        uploader = Some(meta.primary_artist().to_string());
                                        thumbnail = meta.cover_url.clone();
                                        if meta.duration_ms > 0 {
                                            duration_secs = Some(meta.duration_ms / 1000);
                                        }
                                    }
                                }
                            }

                            let client = reqwest::Client::builder()
                                .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36")
                                .build()
                                .unwrap_or_else(|_| reqwest::Client::new());

                            if let Ok(resp) = client.get(&req.url).send().await {
                                if let Ok(html) = resp.text().await {
                                    if thumbnail.is_none() {
                                        if let Some(caps) = regex::Regex::new(r#"<meta property="og:image" content="([^"]+)""#).unwrap().captures(&html) {
                                            thumbnail = Some(caps.get(1).unwrap().as_str().to_string());
                                        }
                                    }
                                    if title == "Spotify Audio" || title == "Spotify Плейлист/Альбом" {
                                        if let Some(caps) = regex::Regex::new(r#"<meta property="og:title" content="([^"]+)""#).unwrap().captures(&html) {
                                            let t = caps.get(1).unwrap().as_str().to_string();
                                            title = t.replace("&amp;", "&").replace("&#39;", "'").replace("&quot;", "\"");
                                            
                                            if !is_playlist {
                                                if title.contains("·") {
                                                    let parts: Vec<&str> = title.split(" · ").collect();
                                                    if parts.len() >= 2 {
                                                        let new_title = parts[1].to_string();
                                                        uploader = Some(parts[0].to_string());
                                                        title = new_title;
                                                    }
                                                } else if title.contains(" - song and lyrics by ") {
                                                    let parts: Vec<&str> = title.split(" - song and lyrics by ").collect();
                                                    if parts.len() == 2 {
                                                        let new_title = parts[0].to_string();
                                                        let artist_part = parts[1].split(" | Spotify").next().unwrap_or(parts[1]);
                                                        uploader = Some(artist_part.to_string());
                                                        title = new_title;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    
                                    if is_playlist {
                                        if let Some((stype, sid)) = crate::audio::spotify::SpotifyClient::parse_spotify_url(&req.url) {
                                            let mut urls = vec![];
                                            if stype == crate::audio::spotify::SpotifyType::Playlist {
                                                if let Ok(u) = spotify_client.get_playlist_track_urls(&ctx_info.config, &sid).await {
                                                    urls = u;
                                                }
                                            } else if stype == crate::audio::spotify::SpotifyType::Album {
                                                if let Ok(u) = spotify_client.get_album_track_urls(&ctx_info.config, &sid).await {
                                                    urls = u;
                                                }
                                            }
                                            for u in urls {
                                                if !playlist_urls.contains(&u) {
                                                    playlist_urls.push(u);
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            let mut filesize_bytes = None;
                            if let Some(ds) = duration_secs {
                                filesize_bytes = Some(ds * 32000);
                            }

                            let mut qual_opt = fsocial_common::QualityOption {
                                quality: fsocial_common::Quality::AudioBest,
                                filesize_bytes,
                                estimated_secs: filesize_bytes.map(|b| (b / (10 * 1024 * 1024)).max(1)),
                                speed_category: "🚀".to_string(),
                                display_label: "🎵 MP3".to_string(),
                                full_button_label: "🎵 MP3".to_string(),
                            };

                            if let Some(sz) = filesize_bytes {
                                let mb = sz / (1024 * 1024);
                                if mb > 0 {
                                    qual_opt.display_label = format!("🎵 MP3 (~{} МБ)", mb);
                                    qual_opt.full_button_label = format!("🎵 MP3  •  ~{} МБ", mb);
                                }
                            }

                            Ok(fsocial_common::InfoResponse {
                                title,
                                uploader,
                                thumbnail,
                                duration_secs,
                                available_qualities: vec![qual_opt],
                                is_playlist,
                                playlist_count: if is_playlist { Some(playlist_urls.len() as u32) } else { None },
                                playlist_urls,
                                error: None,
                            })
                        } else {
                            let mut info_attempts = 0;
                            let max_info_attempts = 3;
                            let mut info_res = Err(fsocial_common::AppError::Download("Failed to get info".into()));
                            
                            while info_attempts < max_info_attempts {
                                info_attempts += 1;
                                let proxy = ctx_info.proxy_pool.next();
                                info_res = media::ytdlp::get_info(&ctx_info.config, &req.url, proxy).await;
                                
                                if info_res.is_ok() {
                                    break;
                                } else if let Err(ref e) = info_res {
                                    if !e.is_retryable() || info_attempts >= max_info_attempts {
                                        break;
                                    }
                                    tracing::warn!("get_info failed on url {} (attempt {}/{}). Retrying... Error: {:?}", req.url, info_attempts, max_info_attempts, e);
                                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                                }
                            }
                            info_res
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
            let mut first_error = None;

            for (idx, url) in urls_to_process.iter().enumerate() {
                if is_playlist_mode {
                    let _ = tx.send(fsocial_common::ProgressEvent::NewTrack(idx, total)).await;
                }
                
                let mut current_task = task.clone();
                current_task.url = url.clone();
                // Clear metadata so spotify processes this single track correctly
                current_task.spotify_meta = None;

                let mut attempts = 0;
                let max_attempts = 3;
                let mut track_success = false;

                while attempts < max_attempts {
                    attempts += 1;
                    
                    let result = if current_task.platform == Platform::Spotify {
                        audio::process_spotify_task(&ctx_clone, &current_task, Some(tx.clone())).await
                    } else {
                        media::process_media_task(&ctx_clone, &current_task, Some(tx.clone())).await
                    };

                    match result {
                        Ok((file_path, title, duration_secs, performer, thumb_path)) => {
                            completed_files.push((file_path, title, duration_secs, performer, thumb_path, current_task.quality.is_audio()));
                            track_success = true;
                            break; // Track downloaded successfully, break retry loop
                        }
                        Err(e) => {
                            if !e.is_retryable() {
                                tracing::warn!("Task {} encountered non-retryable error on url {}: {:?}", task.task_id, url, e);
                                if first_error.is_none() {
                                    first_error = Some(e);
                                }
                                break; // Give up immediately
                            }

                            if attempts >= max_attempts {
                                tracing::warn!("Task {} finally failed on url {} after {} attempts: {:?}", task.task_id, url, attempts, e);
                                if first_error.is_none() {
                                    first_error = Some(e);
                                }
                                break; // Give up on this track
                            }
                            
                            tracing::warn!("Task {} failed on url {} (attempt {}/{}). Retryable error. Waiting 15s... Error: {:?}", task.task_id, url, attempts, max_attempts, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
                        }
                    }
                }

                if !track_success && !is_playlist_mode {
                    break; // If single track failed, abort entirely
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
                    }
                } else if !completed_files.is_empty() {
                    let (file_path, title, duration_secs, performer, thumb_path, is_audio) = completed_files.remove(0);
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
                        },
                    };
                    publish_result(&ctx_clone.nats_client, &res).await;
                    let _ = msg.ack().await;
                }
            }
        });
    }
}
