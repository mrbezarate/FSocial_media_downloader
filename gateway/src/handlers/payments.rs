use teloxide::prelude::*;
use teloxide::types::PreCheckoutQuery;
use tracing::{info, error};

pub async fn handle_pre_checkout_query(bot: crate::MyBot, q: PreCheckoutQuery) -> ResponseResult<()> {
    // Approve all pre-checkout queries for stars
    bot.answer_pre_checkout_query(q.id, true).await?;
    Ok(())
}

pub async fn handle_successful_payment(
    bot: crate::MyBot,
    msg: Message,
    redis_pool: deadpool_redis::Pool,
) -> ResponseResult<()> {
    if let Some(payment) = msg.successful_payment() {
        let user_id = msg.from.as_ref().map(|u| u.id.0).unwrap_or(0);
        let days = if payment.total_amount == 500 { 30 } else { 30 }; // default 1 month
        
        if user_id > 0 {
            if let Ok(mut conn) = redis_pool.get().await {
                let key = format!("user_settings:{}", user_id);
                let mut settings = fsocial_common::UserSettings::default();
                
                let res: redis::RedisResult<String> = redis::cmd("GET").arg(&key).query_async(&mut conn).await;
                if let Ok(val) = res {
                    if let Ok(parsed) = serde_json::from_str::<fsocial_common::UserSettings>(&val) {
                        settings = parsed;
                    }
                }
                
                let now = chrono::Utc::now().timestamp();
                let current_until = settings.premium_until.unwrap_or(now).max(now);
                settings.premium_until = Some(current_until + days * 86400);
                
                if let Ok(json) = serde_json::to_string(&settings) {
                    let _: redis::RedisResult<()> = redis::cmd("SET").arg(&key).arg(json).query_async(&mut conn).await;
                    info!("User {} bought Premium! Updated until: {}", user_id, settings.premium_until.unwrap());
                    bot.send_message(
                        msg.chat.id,
                        "🎉 <b>Оплата успешно получена!</b>\n\nСпасибо за покупку Premium 💎\nТеперь вам доступны неограниченные загрузки без очередей!"
                    ).parse_mode(teloxide::types::ParseMode::Html).await?;
                }
            }
        }
    }
    Ok(())
}
