use crate::{audio, media};
use fsocial_common::subjects;
use futures::StreamExt;
use std::sync::Arc;
use tracing::info;

pub async fn start_info_listener(ctx: Arc<crate::nats_consumer::WorkerContext>) {
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

                            let spotify_client = audio::spotify::SpotifyClient::new();
                            if let Some((stype, sid)) = audio::spotify::SpotifyClient::parse_spotify_url(&req.url) {
                                if stype == audio::spotify::SpotifyType::Track {
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
                                        if let Some((stype, sid)) = audio::spotify::SpotifyClient::parse_spotify_url(&req.url) {
                                            let mut urls = vec![];
                                            if stype == audio::spotify::SpotifyType::Playlist {
                                                if let Ok(u) = spotify_client.get_playlist_track_urls(&ctx_info.config, &sid).await {
                                                    urls = u;
                                                }
                                            } else if stype == audio::spotify::SpotifyType::Album {
                                                if let Ok(u) = spotify_client.get_album_track_urls(&ctx_info.config, &sid).await {
                                                    urls = u;
                                                }
                                            }
                                            
                                            // Fallback to yt-dlp if Spotify API fails (e.g. no credentials)
                                            if urls.is_empty() {
                                                tracing::warn!("Spotify API returned 0 tracks. Falling back to yt-dlp...");
                                                if let Ok(info) = media::ytdlp::get_info(&ctx_info.config, &req.url, ctx_info.proxy_pool.next()).await {
                                                    urls = info.playlist_urls;
                                                }
                                            }

                                            for u in urls {
                                                if !playlist_urls.contains(&u) {
                                                    playlist_urls.push(u);
                                                }
                                            }
                                            
                                            if playlist_urls.len() >= 30 && ctx_info.config.spotify_client_id.is_none() {
                                                title = format!("{} (Показано 30 треков. Добавьте API ключи Spotify в .env для полных плейлистов)", title);
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
}
