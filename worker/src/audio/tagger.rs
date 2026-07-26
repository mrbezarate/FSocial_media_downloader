use fsocial_common::{AppError, SpotifyTrackMeta};
use lofty::config::WriteOptions;
use lofty::file::TaggedFileExt;
use lofty::picture::{MimeType, Picture, PictureType};
use lofty::tag::{Accessor, ItemKey, TagExt};
use reqwest::Client;

pub async fn download_cover(url: &str) -> Result<Vec<u8>, AppError> {
    let client = Client::new();
    let res = client
        .get(url)
        .send()
        .await
        .map_err(|e| AppError::Http(e.to_string()))?;
    if !res.status().is_success() {
        return Err(AppError::Http(format!(
            "Failed to download cover: {}",
            res.status()
        )));
    }
    let bytes = res
        .bytes()
        .await
        .map_err(|e| AppError::Http(e.to_string()))?;
    Ok(bytes.to_vec())
}

pub async fn apply_tags(
    file_path: &str,
    meta: &SpotifyTrackMeta,
    cover_data: Option<Vec<u8>>,
) -> Result<(), AppError> {
    let file_path = file_path.to_string();
    let meta = meta.clone();

    // lofty operations are synchronous — run in blocking task
    tokio::task::spawn_blocking(move || {
        let ext = std::path::Path::new(&file_path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("mp3");
        let temp_path = format!("{}_tmp.{}", file_path, ext);
        std::fs::copy(&file_path, &temp_path)
            .map_err(|e| AppError::Tagging(format!("Failed to copy to temp: {}", e)))?;

        let mut tagged_file = lofty::read_from_path(&temp_path).map_err(|e| {
            let _ = std::fs::remove_file(&temp_path);
            AppError::Tagging(e.to_string())
        })?;

        let tag_type = tagged_file.primary_tag_type();
        let tag = match tagged_file.primary_tag_mut() {
            Some(t) => t,
            None => {
                if let Some(t) = tagged_file.first_tag_mut() {
                    t
                } else {
                    tagged_file.insert_tag(lofty::tag::Tag::new(tag_type));
                    tagged_file.primary_tag_mut().unwrap()
                }
            }
        };

        tag.set_title(meta.title.clone());
        tag.set_artist(meta.artists.join(", "));
        tag.set_album(meta.album.clone());

        if let Some(y) = meta.year {
            tag.insert_text(ItemKey::Year, y.to_string());
        }
        if let Some(t) = meta.track_number {
            tag.set_track(t);
        }
        if !meta.genres.is_empty() {
            tag.set_genre(meta.genres.join(", "));
        }

        if let Some(cover) = cover_data {
            let pic =
                Picture::new_unchecked(PictureType::CoverFront, Some(MimeType::Jpeg), None, cover);
            tag.push_picture(pic);
        }

        if let Err(e) = tag.save_to_path(&temp_path, WriteOptions::default()) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(AppError::Tagging(e.to_string()));
        }

        std::fs::rename(&temp_path, &file_path).map_err(|e| {
            let _ = std::fs::remove_file(&temp_path);
            AppError::Tagging(format!("Failed to rename temp: {}", e))
        })?;

        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::Tagging(format!("Blocking task failed: {}", e)))??;

    Ok(())
}
