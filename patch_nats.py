import os
import re

file_path = "gateway/src/nats_listener.rs"
with open(file_path, "r") as f:
    content = f.read()

# 1. TaskStatus::Completed
content = content.replace("""            let path = PathBuf::from(&file_path);

            if let Ok(meta) = tokio::fs::metadata(&path).await {""", """            let path = PathBuf::from(&file_path);
            let mut _guard = crate::file_guard::FileGuard::new(path.clone());
            if let Some(thumb) = &thumb_path {
                _guard.add(PathBuf::from(thumb));
            }

            if let Ok(meta) = tokio::fs::metadata(&path).await {""")

content = content.replace("""                    let _ = tokio::fs::remove_file(&file_path).await;
                    if let Some(thumb) = &thumb_path {
                        let _ = tokio::fs::remove_file(thumb).await;
                    }
                    return;
                }""", """                    return;
                }""")

content = content.replace("""                    if let Err(e) = tokio::fs::remove_file(&file_path).await {
                        error!("Failed to clean up file {}: {}", file_path, e);
                    }
                    if let Some(thumb) = &thumb_path {
                        let _ = tokio::fs::remove_file(thumb).await;
                    }""", """                    // Handled by _guard""")

# 2. TaskStatus::PlaylistCompleted
content = content.replace("""        TaskStatus::PlaylistCompleted { files, playlist_title, failed_count, failed_items } => {
            tracing::info!("Received PlaylistCompleted for chat {} with {} files", chat_id, files.len());""", """        TaskStatus::PlaylistCompleted { files, playlist_title, failed_count, failed_items } => {
            tracing::info!("Received PlaylistCompleted for chat {} with {} files", chat_id, files.len());
            let mut _guard = crate::file_guard::FileGuard::empty();
            for (file_path, _, _, _, thumb_path, _, _) in &files {
                _guard.add(PathBuf::from(file_path));
                if let Some(thumb) = thumb_path {
                    _guard.add(PathBuf::from(thumb));
                }
            }""")

content = content.replace("""                    // cleanup
                    for (file_path, _, _, _, thumb_path, _, _) in chunk {
                        let _ = tokio::fs::remove_file(file_path).await;
                        if let Some(t) = thumb_path {
                            let _ = tokio::fs::remove_file(t).await;
                        }
                    }""", """                    // cleanup done by _guard""")

content = content.replace("""            for (file_path, _, _, _, thumb_path, _, _) in files {
                let _ = tokio::fs::remove_file(&file_path).await;
                if let Some(thumb) = thumb_path {
                    let _ = tokio::fs::remove_file(thumb).await;
                }
            }""", """            // Cleanup done by _guard""")

# 3. TaskStatus::V2Completed
content = content.replace("""                if output.cleanup == fsocial_common::CleanupStrategy::DeleteAfterDelivery {
                    if let fsocial_common::OutputPayload::Resource { uri } = &output.payload {
                        if let fsocial_common::OutputUri::LocalFile(path_str) = uri {
                            let _ = tokio::fs::remove_file(path_str).await;
                        }
                    }
                    
                    let thumb_uri = match &output.metadata {
                        fsocial_common::OutputMetadata::Video(m) => &m.thumb_uri,
                        fsocial_common::OutputMetadata::Audio(m) => &m.thumb_uri,
                        fsocial_common::OutputMetadata::Image(m) => &m.thumb_uri,
                        _ => &None,
                    };
                    if let Some(fsocial_common::OutputUri::LocalFile(thumb_str)) = thumb_uri {
                        let _ = tokio::fs::remove_file(thumb_str).await;
                    }
                }""", """                let mut _guard = crate::file_guard::FileGuard::empty();
                if output.cleanup == fsocial_common::CleanupStrategy::DeleteAfterDelivery {
                    if let fsocial_common::OutputPayload::Resource { uri } = &output.payload {
                        if let fsocial_common::OutputUri::LocalFile(path_str) = uri {
                            _guard.add(PathBuf::from(path_str));
                        }
                    }
                    
                    let thumb_uri = match &output.metadata {
                        fsocial_common::OutputMetadata::Video(m) => &m.thumb_uri,
                        fsocial_common::OutputMetadata::Audio(m) => &m.thumb_uri,
                        fsocial_common::OutputMetadata::Image(m) => &m.thumb_uri,
                        _ => &None,
                    };
                    if let Some(fsocial_common::OutputUri::LocalFile(thumb_str)) = thumb_uri {
                        _guard.add(PathBuf::from(thumb_str));
                    }
                }""")

with open(file_path, "w") as f:
    f.write(content)
