use fsocial_common::{AppConfig, AppError, SpotifyTrackMeta};
use reqwest::Client;
use serde::Deserialize;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tracing::info;
use base64::{Engine as _, engine::general_purpose::STANDARD as b64};

struct TokenData {
    token: String,
    expires_at: Instant,
}

pub struct SpotifyClient {
    client: Client,
    token_data: RwLock<Option<TokenData>>,
}

#[derive(Debug, PartialEq)]
pub enum SpotifyType {
    Track,
    Album,
    Playlist,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

impl SpotifyClient {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            token_data: RwLock::new(None),
        }
    }

    async fn authenticate(&self, client_id: &str, client_secret: &str) -> Result<(), AppError> {
        let auth_str = format!("{}:{}", client_id, client_secret);
        let b64_auth = b64.encode(auth_str);

        let res = self.client.post("https://accounts.spotify.com/api/token")
            .header("Authorization", format!("Basic {}", b64_auth))
            .form(&[("grant_type", "client_credentials")])
            .send()
            .await
            .map_err(|e| AppError::Spotify(e.to_string()))?;

        if !res.status().is_success() {
            return Err(AppError::Spotify(format!("Auth failed: {}", res.status())));
        }

        let token_res: TokenResponse = res.json().await.map_err(|e| AppError::Spotify(e.to_string()))?;
        
        let mut td = self.token_data.write().unwrap();
        *td = Some(TokenData {
            token: token_res.access_token,
            expires_at: Instant::now() + Duration::from_secs(token_res.expires_in - 60),
        });

        info!("Successfully authenticated with Spotify API");
        Ok(())
    }

    async fn ensure_token(&self, config: &AppConfig) -> Result<String, AppError> {
        let needs_auth = {
            let td = self.token_data.read().unwrap();
            match &*td {
                Some(data) => Instant::now() >= data.expires_at,
                None => true,
            }
        };

        if needs_auth {
            if let (Some(id), Some(secret)) = (&config.spotify_client_id, &config.spotify_client_secret) {
                self.authenticate(id, secret).await?;
            } else {
                return Err(AppError::Spotify("Spotify credentials not configured".into()));
            }
        }

        let td = self.token_data.read().unwrap();
        Ok(td.as_ref().unwrap().token.clone())
    }

    pub async fn get_track(&self, config: &AppConfig, track_id: &str) -> Result<SpotifyTrackMeta, AppError> {
        // Try official API first
        if let Ok(token) = self.ensure_token(config).await {
            let res = self.client.get(format!("https://api.spotify.com/v1/tracks/{}", track_id))
                .bearer_auth(token)
                .send()
                .await;
                
            if let Ok(res) = res {
                if res.status().is_success() {
                    if let Ok(v) = res.json::<serde_json::Value>().await {
                        let title = v["name"].as_str().unwrap_or("Unknown").to_string();
                        let artists = v["artists"].as_array()
                            .map(|a| a.iter().filter_map(|x| x["name"].as_str().map(String::from)).collect())
                            .unwrap_or_default();
                        let album = v["album"]["name"].as_str().unwrap_or("Unknown").to_string();
                        
                        let year = v["album"]["release_date"].as_str()
                            .and_then(|d| d.split('-').next())
                            .and_then(|y| y.parse::<u32>().ok());
                            
                        let cover_url = v["album"]["images"].as_array()
                            .and_then(|arr| arr.first())
                            .and_then(|img| img["url"].as_str())
                            .map(String::from);
                            
                        let track_number = v["track_number"].as_u64().map(|n| n as u32);
                        let duration_ms = v["duration_ms"].as_u64().unwrap_or(0);

                        return Ok(SpotifyTrackMeta {
                            title,
                            artists,
                            album,
                            year,
                            track_number,
                            total_tracks: None,
                            isrc: None,
                            cover_url,
                            duration_ms,
                            genres: vec![],
                        });
                    }
                }
            }
        }
        
        // Fallback: Web Scraping without API key
        info!("Falling back to web scraping for Spotify track: {}", track_id);
        let html = self.client.get(format!("https://open.spotify.com/track/{}", track_id))
            .send()
            .await
            .map_err(|e| AppError::Spotify(e.to_string()))?
            .text()
            .await
            .map_err(|e| AppError::Spotify(e.to_string()))?;

        let title_re = regex::Regex::new(r#"<meta property="og:title" content="([^"]+)"\s*/?>"#).unwrap();
        let desc_re = regex::Regex::new(r#"<meta property="og:description" content="([^"]+)"\s*/?>"#).unwrap();
        let img_re = regex::Regex::new(r#"<meta property="og:image" content="([^"]+)"\s*/?>"#).unwrap();

        let title = title_re.captures(&html).and_then(|c| c.get(1)).map(|m| m.as_str().to_string()).unwrap_or_else(|| "Unknown".to_string());
        let desc = desc_re.captures(&html).and_then(|c| c.get(1)).map(|m| m.as_str().to_string()).unwrap_or_default();
        let cover_url = img_re.captures(&html).and_then(|c| c.get(1)).map(|m| m.as_str().to_string());

        // desc is usually "Artist Name · Song · 2023"
        let parts: Vec<&str> = desc.split(" · ").collect();
        let artist_str = parts.first().unwrap_or(&"Unknown");
        let artists: Vec<String> = artist_str.split(", ").map(|s| s.to_string()).collect();
        let year = parts.last().and_then(|s| s.parse::<u32>().ok());

        Ok(SpotifyTrackMeta {
            title,
            artists,
            album: "Spotify Single".to_string(), // Fallback album name
            year,
            track_number: None,
            total_tracks: None,
            isrc: None,
            cover_url,
            duration_ms: 0,
            genres: vec![],
        })
    }

    #[allow(dead_code)]
    pub async fn get_playlist_tracks(&self, config: &AppConfig, playlist_id: &str) -> Result<Vec<SpotifyTrackMeta>, AppError> {
        let token = self.ensure_token(config).await?;
        // simplified pagination for example
        let res = self.client.get(format!("https://api.spotify.com/v1/playlists/{}/tracks?limit=100", playlist_id))
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| AppError::Spotify(e.to_string()))?;

        if !res.status().is_success() {
            return Err(AppError::Spotify(format!("API error: {}", res.status())));
        }

        let v: serde_json::Value = res.json().await.map_err(|e| AppError::Spotify(e.to_string()))?;
        let mut tracks = Vec::new();
        
        if let Some(items) = v["items"].as_array() {
            for item in items {
                if let Some(track) = item.get("track") {
                    let title = track["name"].as_str().unwrap_or("Unknown").to_string();
                    let artists = track["artists"].as_array()
                        .map(|a| a.iter().filter_map(|x| x["name"].as_str().map(String::from)).collect())
                        .unwrap_or_default();
                    let album = track["album"]["name"].as_str().unwrap_or("Unknown").to_string();
                    
                    let cover_url = track["album"]["images"].as_array()
                        .and_then(|arr| arr.first())
                        .and_then(|img| img["url"].as_str())
                        .map(String::from);

                    tracks.push(SpotifyTrackMeta {
                        title,
                        artists,
                        album,
                        year: None,
                        track_number: None,
                        total_tracks: None,
                        isrc: None,
                        cover_url,
                        duration_ms: track["duration_ms"].as_u64().unwrap_or(0),
                        genres: vec![],
                    });
                }
            }
        }
        Ok(tracks)
    }

    #[allow(dead_code)]
    pub async fn get_album_tracks(&self, _config: &AppConfig, _album_id: &str) -> Result<Vec<SpotifyTrackMeta>, AppError> {
        // Full pagination implementation would mirror get_playlist_tracks
        Ok(vec![])
    }

    pub async fn get_playlist_track_urls(&self, config: &AppConfig, playlist_id: &str) -> Result<Vec<String>, AppError> {
        let mut urls = Vec::new();
        if let Ok(token) = self.ensure_token(config).await {
            let mut url = format!("https://api.spotify.com/v1/playlists/{}/tracks?limit=100", playlist_id);

            loop {
                let res = self.client.get(&url)
                    .bearer_auth(&token)
                    .send()
                    .await;

                if let Ok(res) = res {
                    if res.status().is_success() {
                        if let Ok(v) = res.json::<serde_json::Value>().await {
                            if let Some(items) = v["items"].as_array() {
                                for item in items {
                                    if let Some(track) = item.get("track") {
                                        if let Some(id) = track["id"].as_str() {
                                            let track_url = format!("https://open.spotify.com/track/{}", id);
                                            if !urls.contains(&track_url) {
                                                urls.push(track_url);
                                            }
                                        }
                                    }
                                }
                            }

                            if let Some(next) = v["next"].as_str() {
                                url = next.to_string();
                                continue;
                            }
                        }
                    }
                }
                break;
            }
        }
        
        if urls.is_empty() {
            info!("Falling back to web scraping for Spotify playlist: {}", playlist_id);
            if let Ok(html) = self.client.get(format!("https://open.spotify.com/playlist/{}", playlist_id))
                .send().await.and_then(|r| r.error_for_status()) {
                if let Ok(text) = html.text().await {
                    let re = regex::Regex::new(r"https://open\.spotify\.com/track/[a-zA-Z0-9]+").unwrap();
                    for mat in re.find_iter(&text) {
                        let u = mat.as_str().to_string();
                        if !urls.contains(&u) {
                            urls.push(u);
                        }
                    }
                }
            }
        }
        Ok(urls)
    }

    pub async fn get_album_track_urls(&self, config: &AppConfig, album_id: &str) -> Result<Vec<String>, AppError> {
        let mut urls = Vec::new();
        if let Ok(token) = self.ensure_token(config).await {
            let mut url = format!("https://api.spotify.com/v1/albums/{}/tracks?limit=50", album_id);

            loop {
                let res = self.client.get(&url)
                    .bearer_auth(&token)
                    .send()
                    .await;

                if let Ok(res) = res {
                    if res.status().is_success() {
                        if let Ok(v) = res.json::<serde_json::Value>().await {
                            if let Some(items) = v["items"].as_array() {
                                for track in items {
                                    if let Some(id) = track["id"].as_str() {
                                        let track_url = format!("https://open.spotify.com/track/{}", id);
                                        if !urls.contains(&track_url) {
                                            urls.push(track_url);
                                        }
                                    }
                                }
                            }

                            if let Some(next) = v["next"].as_str() {
                                url = next.to_string();
                                continue;
                            }
                        }
                    }
                }
                break;
            }
        }

        if urls.is_empty() {
            info!("Falling back to web scraping for Spotify album: {}", album_id);
            if let Ok(html) = self.client.get(format!("https://open.spotify.com/album/{}", album_id))
                .send().await.and_then(|r| r.error_for_status()) {
                if let Ok(text) = html.text().await {
                    let re = regex::Regex::new(r"https://open\.spotify\.com/track/[a-zA-Z0-9]+").unwrap();
                    for mat in re.find_iter(&text) {
                        let u = mat.as_str().to_string();
                        if !urls.contains(&u) {
                            urls.push(u);
                        }
                    }
                }
            }
        }
        Ok(urls)
    }

    pub fn parse_spotify_url(url: &str) -> Option<(SpotifyType, String)> {
        let re = regex::Regex::new(r"open\.spotify\.com/(track|album|playlist)/([a-zA-Z0-9]+)").unwrap();
        if let Some(caps) = re.captures(url) {
            let t = match &caps[1] {
                "track" => SpotifyType::Track,
                "album" => SpotifyType::Album,
                "playlist" => SpotifyType::Playlist,
                _ => return None,
            };
            return Some((t, caps[2].to_string()));
        }
        None
    }
}
