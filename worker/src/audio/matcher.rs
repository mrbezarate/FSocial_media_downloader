use fsocial_common::{AppConfig, AppError, SpotifyTrackMeta};
use std::process::Stdio;
use tokio::process::Command;
use serde_json::Value;
use tracing::info;

pub async fn find_on_youtube(config: &AppConfig, meta: &SpotifyTrackMeta, proxy: Option<&str>) -> Result<String, AppError> {
    let query = meta.youtube_search_query();
    info!("Searching YouTube for: {}", query);

    let mut cmd = Command::new(&config.ytdlp_path);
    cmd.arg("--dump-json")
       .arg("--no-download")
       .arg("--default-search").arg("ytsearch1");
       
    if let Some(p) = proxy {
        cmd.arg("--proxy").arg(p);
    }
    
    if let Some(ref cookies) = config.cookies_path {
        cmd.arg("--cookies").arg(cookies);
    }

    cmd.arg(&query);

    let output = cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).output().await?;

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
        return Err(AppError::Download("YouTube search returned empty output. Rate limit or IP ban suspected.".into()));
    }
    
    let json: Value = serde_json::from_str(first_line)?;

    let url = json.get("webpage_url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Download("Failed to get webpage_url from search result".into()))?
        .to_string();

    Ok(url)
}
