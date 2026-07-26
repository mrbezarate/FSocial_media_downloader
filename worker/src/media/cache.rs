use deadpool_redis::Pool;
use fsocial_common::AppError;
use moka::future::Cache;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedMedia {
    pub file_path: String,
    pub title: String,
    pub duration: Option<u64>,
}

#[derive(Clone)]
pub struct MetadataCache {
    l1: Cache<String, CachedMedia>,
    redis_pool: Pool,
}

impl MetadataCache {
    pub fn new(redis_pool: Pool) -> Self {
        let l1 = Cache::builder()
            .time_to_live(Duration::from_secs(21600))
            .max_capacity(1000)
            .build();
        Self { l1, redis_pool }
    }

    pub async fn get(&self, url: &str) -> Option<CachedMedia> {
        let mut cached_val = None;
        if let Some(cached) = self.l1.get(url).await {
            cached_val = Some(cached);
        } else if let Ok(mut conn) = self.redis_pool.get().await {
            let key = format!("media:{}", url);
            if let Ok(json_str) = conn.get::<_, String>(&key).await {
                if let Ok(cached) = serde_json::from_str::<CachedMedia>(&json_str) {
                    self.l1.insert(url.to_string(), cached.clone()).await;
                    cached_val = Some(cached);
                }
            }
        }

        if let Some(cached) = cached_val {
            if tokio::fs::metadata(&cached.file_path).await.is_ok() {
                return Some(cached);
            } else {
                self.l1.invalidate(url).await;
                if let Ok(mut conn) = self.redis_pool.get().await {
                    let key = format!("media:{}", url);
                    let _: () = redis::cmd("DEL")
                        .arg(key)
                        .query_async(&mut conn)
                        .await
                        .unwrap_or(());
                }
            }
        }
        None
    }

    pub async fn set(&self, url: &str, media: CachedMedia) -> Result<(), AppError> {
        self.l1.insert(url.to_string(), media.clone()).await;

        let mut conn = self
            .redis_pool
            .get()
            .await
            .map_err(|e| AppError::Redis(e.to_string()))?;
        let key = format!("media:{}", url);
        let json_str = serde_json::to_string(&media)?;

        let _: () = conn
            .set_ex(&key, json_str, 21600)
            .await
            .map_err(|e| AppError::Redis(e.to_string()))?;
        Ok(())
    }
}
