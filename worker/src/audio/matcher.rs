use fsocial_common::{AppConfig, AppError, SpotifyTrackMeta};
use serde_json::Value;
use std::process::Stdio;
use tokio::process::Command;
use tracing::info;

pub async fn find_track_url(
    config: &AppConfig,
    meta: &SpotifyTrackMeta,
    proxy: Option<&str>,
) -> Result<String, AppError> {
    let query = meta.youtube_search_query();
    info!("Searching platforms for: {}", query);

    // Try YouTube Music first (already baked into query in models.rs)
    if let Ok(url) = search_platform(config, &query, "", proxy).await {
        return Ok(url);
    }

    // Try standard YouTube as fallback
    let fallback_query = query.replace("ytmsearch1:", "ytsearch1:");
    if let Ok(url) = search_platform(config, &fallback_query, "", proxy).await {
        return Ok(url);
    }

    tracing::warn!(
        "YouTube search failed for {}. Falling back to SoundCloud...",
        query
    );

    // Fallback to SoundCloud
    let sc_query = fallback_query.replace("ytsearch1:", "scsearch1:");
    if let Ok(url) = search_platform(config, &sc_query, "", proxy).await {
        return Ok(url);
    }

    Err(AppError::Download(
        "Could not find track on YouTube or SoundCloud".into(),
    ))
}

async fn search_platform(
    config: &AppConfig,
    query: &str,
    search_prefix: &str,
    proxy: Option<&str>,
) -> Result<String, AppError> {
    let mut cmd = Command::new(&config.ytdlp_path);
    cmd.arg("--dump-json").arg("--no-download");

    if !search_prefix.is_empty() {
        cmd.arg("--default-search").arg(search_prefix);
    }

    if let Some(p) = proxy {
        cmd.arg("--proxy").arg(p);
    }

    if let Some(ref cookies) = config.cookies_path {
        cmd.arg("--cookies").arg(cookies);
    }

    cmd.arg(query);

    let output = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(AppError::YtDlp {
            message: stderr.into_owned(),
            exit_code: output.status.code().unwrap_or(-1),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let first_line = stdout.lines().next().unwrap_or_default();

    if first_line.is_empty() {
        return Err(AppError::Download(format!(
            "{} search returned empty output. Rate limit or IP ban suspected.",
            search_prefix
        )));
    }

    let json: Value = serde_json::from_str(first_line)?;

    let url = json
        .get("webpage_url")
        .or_else(|| json.get("url"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            AppError::Download(format!(
                "Failed to get URL from {} search result",
                search_prefix
            ))
        })?
        .to_string();

    Ok(url)
}
