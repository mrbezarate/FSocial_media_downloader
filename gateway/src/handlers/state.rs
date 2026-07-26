use teloxide::prelude::*;

pub async fn handle(
    bot: crate::MyBot,
    msg: Message,
    redis_pool: deadpool_redis::Pool,
) -> ResponseResult<()> {
    let user_id = msg.from.as_ref().map(|u| u.id.0).unwrap_or(0);
    if user_id == 0 {
        return Ok(());
    }

    let mut state = String::new();
    if let Ok(mut conn) = redis_pool.get().await {
        let key = format!("admin_state:{}", user_id);
        state = redis::cmd("GET").arg(&key).query_async(&mut conn).await.unwrap_or_default();
    }

    if state.is_empty() {
        // If not in state, and it doesn't contain a URL (download handler), we can just ignore or say something.
        // Wait, if we return Ok(()), it consumes the message, so we must return a Continue if we want download handler to run?
        // Actually, dptree::endpoint consumes it. We should use dptree::filter to only match when state is NOT empty!
        return Ok(());
    }

    let text = msg.text().unwrap_or("").trim();
    if text.is_empty() {
        return Ok(());
    }

    if text == "/cancel" {
        if let Ok(mut conn) = redis_pool.get().await {
            let key = format!("admin_state:{}", user_id);
            let _: redis::RedisResult<()> = redis::cmd("DEL").arg(&key).query_async(&mut conn).await;
        }
        bot.send_message(msg.chat.id, "Действие отменено.").await?;
        return Ok(());
    }

    if let Ok(mut conn) = redis_pool.get().await {
        if state == "waiting_broadcast" {
            bot.send_message(msg.chat.id, "Рассылка запущена...").await?;
            let keys: Vec<String> = redis::cmd("KEYS").arg("user_settings:*").query_async(&mut conn).await.unwrap_or_default();
            let mut success = 0;
            for key in &keys {
                if let Some(uid_str) = key.strip_prefix("user_settings:") {
                    if let Ok(uid) = uid_str.parse::<i64>() {
                        if bot.send_message(teloxide::types::ChatId(uid), text).await.is_ok() {
                            success += 1;
                        }
                    }
                }
            }
            bot.send_message(msg.chat.id, format!("📢 Рассылка завершена! Доставлено: {}/{}", success, keys.len())).await?;
            let _: redis::RedisResult<()> = redis::cmd("DEL").arg(format!("admin_state:{}", user_id)).query_async(&mut conn).await;
        } else if state == "waiting_admin_add" {
            if let Ok(new_admin) = text.parse::<u64>() {
                let _: redis::RedisResult<()> = redis::cmd("SADD").arg("admins:set").arg(new_admin).query_async(&mut conn).await;
                bot.send_message(msg.chat.id, format!("Пользователь {} назначен администратором.", new_admin)).await?;
            } else {
                bot.send_message(msg.chat.id, "Неверный ID пользователя. Попробуйте еще раз или /cancel.").await?;
                return Ok(()); // keep state
            }
            let _: redis::RedisResult<()> = redis::cmd("DEL").arg(format!("admin_state:{}", user_id)).query_async(&mut conn).await;
        } else if state == "waiting_admin_remove" {
            if let Ok(old_admin) = text.parse::<u64>() {
                let _: redis::RedisResult<()> = redis::cmd("SREM").arg("admins:set").arg(old_admin).query_async(&mut conn).await;
                bot.send_message(msg.chat.id, format!("Пользователь {} удален из администраторов.", old_admin)).await?;
            } else {
                bot.send_message(msg.chat.id, "Неверный ID пользователя. Попробуйте еще раз или /cancel.").await?;
                return Ok(());
            }
            let _: redis::RedisResult<()> = redis::cmd("DEL").arg(format!("admin_state:{}", user_id)).query_async(&mut conn).await;
        } else if state == "waiting_promo_create" {
            // expected format: CODE DAYS USES
            let parts: Vec<&str> = text.split_whitespace().collect();
            if parts.len() == 3 {
                let code = parts[0];
                if let (Ok(days), Ok(uses)) = (parts[1].parse::<i64>(), parts[2].parse::<i64>()) {
                    let promo_data = serde_json::json!({
                        "days": days,
                        "max_uses": uses,
                        "uses": 0
                    });
                    let _: redis::RedisResult<()> = redis::cmd("HSET").arg("promocodes").arg(code).arg(promo_data.to_string()).query_async(&mut conn).await;
                    bot.send_message(msg.chat.id, format!("✅ Промокод <code>{}</code> создан!\nДней: {}\nМакс. использований: {}", code, days, uses)).parse_mode(teloxide::types::ParseMode::Html).await?;
                    let _: redis::RedisResult<()> = redis::cmd("DEL").arg(format!("admin_state:{}", user_id)).query_async(&mut conn).await;
                    return Ok(());
                }
            }
            bot.send_message(msg.chat.id, "Неверный формат. Ожидается: КОД ДНИ ИСПОЛЬЗОВАНИЯ\nПример: VIP 30 100\nИли нажмите /cancel").await?;
            return Ok(());
        }
    }

    Ok(())
}
