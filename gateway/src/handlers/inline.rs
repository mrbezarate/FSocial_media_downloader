use fsocial_common::UserSettings;
use teloxide::prelude::*;
use teloxide::types::{
    InlineQueryResult, InlineQueryResultArticle, InlineQueryResultCachedAudio,
    InlineQueryResultCachedVideo, InputMessageContent, InputMessageContentText,
};
use uuid::Uuid;

pub async fn handle(
    bot: crate::MyBot,
    q: InlineQuery,
    redis_pool: deadpool_redis::Pool,
) -> ResponseResult<()> {
    let url = q.query.trim();
    if url.is_empty() {
        return bot.answer_inline_query(q.id, vec![]).await.map(|_| ());
    }

    let url_match = if let Some(m) = crate::url_parser::detect(url) {
        m
    } else {
        return bot.answer_inline_query(q.id, vec![]).await.map(|_| ());
    };

    let user_id = q.from.id.0;
    let mut settings = UserSettings::default();
    if let Ok(mut conn) = redis_pool.get().await {
        let key = format!("user_settings:{}", user_id);
        let res: redis::RedisResult<String> =
            redis::cmd("GET").arg(&key).query_async(&mut conn).await;
        if let Ok(val) = res {
            if let Ok(parsed) = serde_json::from_str::<UserSettings>(&val) {
                settings = parsed;
            }
        }
    }

    let is_audio = url_match.media_type == fsocial_common::MediaType::Audio;
    let quality = if is_audio {
        settings.default_audio
    } else {
        settings.default_video
    };

    let cache_key = format!("file_id:{}:{}", quality.callback_id(), url_match.url);

    let mut cached_file_id = None;
    if let Ok(mut conn) = redis_pool.get().await {
        let res: redis::RedisResult<String> = redis::cmd("GET")
            .arg(&cache_key)
            .query_async(&mut conn)
            .await;
        if let Ok(fid) = res {
            cached_file_id = Some(fid);
        }
    }

    let mut results = vec![];
    let bot_watermark = "Скачано с помощью бота @FSocial_Media_Downloader_bot";
    let result_id = Uuid::new_v4().to_string();

    if let Some(file_id_str) = cached_file_id {
        let file_id = teloxide::types::FileId(file_id_str);
        if is_audio {
            results.push(InlineQueryResult::CachedAudio(
                InlineQueryResultCachedAudio::new(result_id, file_id).caption(bot_watermark),
            ));
        } else {
            results.push(InlineQueryResult::CachedVideo(
                InlineQueryResultCachedVideo::new(result_id, file_id, "Видео".to_string())
                    .caption(bot_watermark),
            ));
        }
    } else {
        let text = format!(
            "Похоже, это медиа еще не скачивалось или истекло.\nПожалуйста, отправьте ссылку напрямую боту: @FSocial_Media_Downloader_bot"
        );
        results.push(InlineQueryResult::Article(
            InlineQueryResultArticle::new(
                result_id,
                "Скачать видео/аудио (еще не в кэше)",
                InputMessageContent::Text(InputMessageContentText::new(text)),
            )
            .description("Медиа не найдено в быстром кэше. Скачайте через бота."),
        ));
    }

    bot.answer_inline_query(q.id, results).await?;

    Ok(())
}
