use teloxide::{prelude::*, utils::command::BotCommands};

#[derive(BotCommands, Clone, Debug)]
#[command(rename_rule = "lowercase", description = "Поддерживаемые команды:")]
pub enum Command {
    #[command(description = "Показать приветствие и список поддерживаемых платформ.")]
    Start,
    #[command(description = "Подробная помощь и примеры.")]
    Help,
    #[command(description = "Показать текущее качество по умолчанию.")]
    Quality,
    #[command(description = "Показать сводку настроек.")]
    Settings,
    #[command(description = "Купить Premium 💎")]
    Premium,
    #[command(description = "Панель администратора")]
    Admin(String),
    #[command(description = "Активировать промокод")]
    Promo(String),
    #[command(description = "Посмотреть логи")]
    Log,
}

pub async fn handle(
    bot: crate::MyBot,
    msg: Message,
    cmd: Command,
    redis_pool: deadpool_redis::Pool,
) -> ResponseResult<()> {
    match cmd {
        Command::Start => {
            let text = "👋 <b>Привет! Я FSocial Media Downloader</b>\n—\n\
                        Поддерживаемые платформы:\n\
                        • YouTube (Видео, Shorts)\n\
                        • TikTok\n\
                        • Instagram (Reels, Посты)\n\
                        • Spotify (Треки, Плейлисты)\n\
                        • SoundCloud\n\
                        • Pinterest\n\n\
                        Просто отправь ссылку для начала загрузки!";
            bot.send_message(msg.chat.id, text)
                .parse_mode(teloxide::types::ParseMode::Html)
                .await?;
        }
        Command::Help => {
            let text = "📖 <b>Как использовать:</b>\n\n\
                        1. Отправьте ссылку на видео/аудио в личные сообщения.\n\
                        2. Выберите качество из появившегося меню.\n\
                        3. Дождитесь окончания загрузки.\n\n\
                        В группах загрузка происходит автоматически, согласно настройкам по умолчанию (см. /settings).";
            bot.send_message(msg.chat.id, text)
                .parse_mode(teloxide::types::ParseMode::Html)
                .await?;
        }
        Command::Quality => {
            let text = "Команда /quality устарела. Пожалуйста, используйте /settings.";
            bot.send_message(msg.chat.id, text).await?;
        }
        Command::Settings => {
            let user_id = msg.from.as_ref().map(|u| u.id.0).unwrap_or(0);

            let mut settings = fsocial_common::UserSettings::default();
            if let Ok(mut conn) = redis_pool.get().await {
                let key = format!("user_settings:{}", user_id);
                let res: redis::RedisResult<String> =
                    redis::cmd("GET").arg(&key).query_async(&mut conn).await;
                if let Ok(val) = res {
                    if let Ok(parsed) = serde_json::from_str::<fsocial_common::UserSettings>(&val) {
                        settings = parsed;
                    }
                }
            }

            let text =
                "🔧 <b>Настройки профиля</b>\n\nЗдесь вы можете настроить параметры загрузок.";

            use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};

            let mut keyboard_rows = Vec::new();

            if settings.auto_download {
                keyboard_rows.push(vec![InlineKeyboardButton::callback(
                    "⚡ Авто",
                    "toggle_mode",
                )]);
                keyboard_rows.push(vec![InlineKeyboardButton::callback(
                    format!("📹 Видео: {:?}", settings.default_video),
                    "setmenu|vid",
                )]);
                keyboard_rows.push(vec![InlineKeyboardButton::callback(
                    format!("🎵 Аудио: {:?}", settings.default_audio),
                    "setmenu|aud",
                )]);
            } else {
                keyboard_rows.push(vec![InlineKeyboardButton::callback(
                    "💬 Ручной",
                    "toggle_mode",
                )]);
            }

            keyboard_rows.push(vec![InlineKeyboardButton::callback(
                "💎 Premium",
                "buy_premium",
            )]);

            let keyboard = InlineKeyboardMarkup::new(keyboard_rows);

            bot.send_message(msg.chat.id, text)
                .parse_mode(teloxide::types::ParseMode::Html)
                .reply_markup(keyboard)
                .await?;
        }
        Command::Premium => {
            let user_id = msg.from.as_ref().map(|u| u.id.0).unwrap_or(0);
            let mut status_text = String::from("\n\n✨ <b>Твой статус:</b> Отсутствует");
            if let Ok(mut conn) = redis_pool.get().await {
                let key = format!("user_settings:{}", user_id);
                let res: redis::RedisResult<String> = redis::cmd("GET").arg(&key).query_async(&mut conn).await;
                if let Ok(val) = res {
                    if let Ok(settings) = serde_json::from_str::<fsocial_common::UserSettings>(&val) {
                        let now = chrono::Utc::now().timestamp();
                        if let Some(until) = settings.premium_until {
                            if until > now {
                                let remaining_secs = until - now;
                                let days = remaining_secs / 86400;
                                let hours = (remaining_secs % 86400) / 3600;
                                status_text = format!("\n\n✨ <b>Твой статус:</b> Активен\n⏳ <b>Осталось:</b> {} дн. {} ч.", days, hours);
                            } else {
                                status_text = format!("\n\n✨ <b>Твой статус:</b> Закончился");
                            }
                        }
                    }
                }
            }
            let text = format!("💎 <b>Premium Подписка</b>{}\n\nВыберите период подписки:\n\n• <b>1 День</b> - 20 ⭐ (попробовать!)\n• <b>1 Месяц</b> - 500 ⭐\n• <b>1 Год</b> - 4800 ⭐ (Выгода 20%!)", status_text);
            let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![
                vec![teloxide::types::InlineKeyboardButton::callback("1 День (20 ⭐)", "invoice|day")],
                vec![teloxide::types::InlineKeyboardButton::callback("1 Месяц (500 ⭐)", "invoice|month")],
                vec![teloxide::types::InlineKeyboardButton::callback("1 Год (4800 ⭐)", "invoice|year")],
                vec![teloxide::types::InlineKeyboardButton::callback("🔙 Назад", "settings_main")],
            ]);
            bot.send_message(msg.chat.id, text)
                .reply_markup(keyboard)
                .parse_mode(teloxide::types::ParseMode::Html)
                .await?;
        }
        Command::Log => {
            let user_id = msg.from.as_ref().map(|u| u.id.0).unwrap_or(0);
            let admin_id_str = std::env::var("ADMIN_ID").unwrap_or_default();
            let admin_id: u64 = admin_id_str.parse().unwrap_or(0);
            let is_dev = msg.from.as_ref()
                .and_then(|u| u.username.as_deref())
                .map(|name| name.eq_ignore_ascii_case("UndaOn"))
                .unwrap_or(false);

            let mut is_admin = user_id != 0 && (user_id == admin_id || is_dev);

            if !is_admin && user_id != 0 {
                if let Ok(mut conn) = redis_pool.get().await {
                    let is_in_set: bool = redis::cmd("SISMEMBER").arg("admins:set").arg(user_id).query_async(&mut conn).await.unwrap_or(false);
                    if is_in_set {
                        is_admin = true;
                    }
                }
            }

            if !is_admin {
                return Ok(());
            }

            let mut logs_text = String::new();
            if let Ok(buf) = crate::admin_logs::LOG_BUFFER.read() {
                let last_40 = buf.iter().rev().take(40).collect::<Vec<_>>();
                for log in last_40.iter().rev() {
                    logs_text.push_str(log);
                    logs_text.push('\n');
                }
            }
            if logs_text.is_empty() {
                logs_text = "Логи пусты.".to_string();
            }
            let text = format!("📜 <b>Последние логи (40 строк):</b>\n\n<pre>{}</pre>", logs_text);
            bot.send_message(msg.chat.id, text).parse_mode(teloxide::types::ParseMode::Html).await?;
        }
        Command::Admin(args) => {
            let admin_id_str = std::env::var("ADMIN_ID").unwrap_or_default();
            let admin_id: u64 = admin_id_str.parse().unwrap_or(0);
            let user = msg.from.as_ref();
            let user_id = user.map(|u| u.id.0).unwrap_or(0);
            let is_dev = user
                .and_then(|u| u.username.as_deref())
                .map(|name| name.eq_ignore_ascii_case("UndaOn"))
                .unwrap_or(false);

            let mut is_admin = user_id != 0 && (user_id == admin_id || is_dev);
            if !is_admin && user_id != 0 {
                if let Ok(mut conn) = redis_pool.get().await {
                    let is_member: bool = redis::cmd("SISMEMBER")
                        .arg("admins:set")
                        .arg(user_id)
                        .query_async(&mut conn)
                        .await
                        .unwrap_or(false);
                    if is_member {
                        is_admin = true;
                    }
                }
            }

            if !is_admin {
                return Ok(());
            }

            let parts: Vec<&str> = args.split_whitespace().collect();
            if parts.is_empty() {
                let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![
                    vec![
                        teloxide::types::InlineKeyboardButton::callback("📊 Статистика", "admin|stats"),
                        teloxide::types::InlineKeyboardButton::callback("📜 Логи системы", "admin|logs"),
                    ],
                    vec![
                        teloxide::types::InlineKeyboardButton::callback("🎟 Промокоды", "admin|promo"),
                        teloxide::types::InlineKeyboardButton::callback("👑 Админы", "admin|admins_menu"),
                    ],
                    vec![
                        teloxide::types::InlineKeyboardButton::callback("📢 Рассылка", "admin|reklama"),
                    ],
                ]);
                bot.send_message(msg.chat.id, "🛠 <b>Dev Tool / Панель Управления</b>\n\nВыберите раздел для управления системой:")
                    .reply_markup(keyboard)
                    .parse_mode(teloxide::types::ParseMode::Html)
                    .await?;
                return Ok(());
            }

            if parts[0] == "add" && parts.len() == 2 {
                if let Ok(new_admin) = parts[1].parse::<u64>() {
                    if let Ok(mut conn) = redis_pool.get().await {
                        let _: redis::RedisResult<()> = redis::cmd("SADD").arg("admins:set").arg(new_admin).query_async(&mut conn).await;
                        bot.send_message(msg.chat.id, format!("Пользователь {} назначен администратором.", new_admin)).await?;
                    }
                }
                return Ok(());
            }

            if parts[0] == "remove" && parts.len() == 2 {
                if let Ok(old_admin) = parts[1].parse::<u64>() {
                    if let Ok(mut conn) = redis_pool.get().await {
                        let _: redis::RedisResult<()> = redis::cmd("SREM").arg("admins:set").arg(old_admin).query_async(&mut conn).await;
                        bot.send_message(msg.chat.id, format!("Пользователь {} удален из администраторов.", old_admin)).await?;
                    }
                }
                return Ok(());
            }

            if parts[0] == "promo_create" && parts.len() == 4 {
                let code = parts[1];
                if let (Ok(days), Ok(uses)) = (parts[2].parse::<i64>(), parts[3].parse::<i64>()) {
                    let promo_data = serde_json::json!({
                        "days": days,
                        "max_uses": uses,
                        "uses": 0
                    });
                    if let Ok(mut conn) = redis_pool.get().await {
                        let _: redis::RedisResult<()> = redis::cmd("HSET").arg("promocodes").arg(code).arg(promo_data.to_string()).query_async(&mut conn).await;
                        bot.send_message(msg.chat.id, format!("✅ Промокод <code>{}</code> создан!\nДней: {}\nМакс. использований: {}", code, days, uses)).parse_mode(teloxide::types::ParseMode::Html).await?;
                    }
                }
                return Ok(());
            }

            if parts[0] == "broadcast" && parts.len() > 1 {
                let text = parts[1..].join(" ");
                if let Ok(mut conn) = redis_pool.get().await {
                    let keys: Vec<String> = redis::cmd("KEYS").arg("user_settings:*").query_async(&mut conn).await.unwrap_or_default();
                    let mut success = 0;
                    for key in &keys {
                        if let Some(uid_str) = key.strip_prefix("user_settings:") {
                            if let Ok(uid) = uid_str.parse::<i64>() {
                                if bot.send_message(teloxide::types::ChatId(uid), &text).await.is_ok() {
                                    success += 1;
                                }
                            }
                        }
                    }
                    bot.send_message(msg.chat.id, format!("📢 Рассылка завершена! Доставлено: {}/{}", success, keys.len())).await?;
                }
                return Ok(());
            }

            if parts[0] == "give_premium" && parts.len() == 3 {
                if let (Ok(target_id), Ok(days)) =
                    (parts[1].parse::<u64>(), parts[2].parse::<i64>())
                {
                    if let Ok(mut conn) = redis_pool.get().await {
                        let key = format!("user_settings:{}", target_id);
                        let mut target_settings = fsocial_common::UserSettings::default();

                        let res: redis::RedisResult<String> =
                            redis::cmd("GET").arg(&key).query_async(&mut conn).await;
                        if let Ok(val) = res {
                            if let Ok(parsed) =
                                serde_json::from_str::<fsocial_common::UserSettings>(&val)
                            {
                                target_settings = parsed;
                            }
                        }

                        let now = chrono::Utc::now().timestamp();
                        let current_until = target_settings.premium_until.unwrap_or(now).max(now);
                        target_settings.premium_until = Some(current_until + days * 86400);

                        if let Ok(json) = serde_json::to_string(&target_settings) {
                            let _: redis::RedisResult<()> = redis::cmd("SET")
                                .arg(&key)
                                .arg(json)
                                .query_async(&mut conn)
                                .await;
                            bot.send_message(
                                msg.chat.id,
                                format!(
                                    "Успешно выдано {} дней Premium пользователю {}",
                                    days, target_id
                                ),
                            )
                            .await?;
                            if let Err(e) = bot
                                .send_message(
                                    teloxide::types::ChatId(target_id as i64),
                                    format!("🎉 Вам был выдан Premium на {} дней от Администрации!", days),
                                )
                                .await
                            {
                                tracing::warn!("Не удалось уведомить пользователя {} о выдаче Premium: {}", target_id, e);
                            }
                            return Ok(());
                        }
                    }
                }
            }

            bot.send_message(msg.chat.id, "Команда не распознана.\nДоступно: /admin, /admin add <id>, /admin remove <id>, /admin promo_create <code> <days> <uses>, /admin give_premium <id> <days>, /admin broadcast <текст>")
                .await?;
        }
        Command::Promo(code) => {
            let code = code.trim();
            if code.is_empty() {
                bot.send_message(msg.chat.id, "Использование: /promo <код>").await?;
                return Ok(());
            }

            let user_id = msg.from.as_ref().map(|u| u.id.0).unwrap_or(0);
            if user_id == 0 { return Ok(()); }

            if let Ok(mut conn) = redis_pool.get().await {
                // Check if user already used this promo
                let used_key = format!("promo_users:{}", code);
                let already_used: bool = redis::cmd("SISMEMBER").arg(&used_key).arg(user_id).query_async(&mut conn).await.unwrap_or(false);
                if already_used {
                    bot.send_message(msg.chat.id, "Вы уже активировали этот промокод.").await?;
                    return Ok(());
                }

                // Check promo existence and stats
                let promo_str: Option<String> = redis::cmd("HGET").arg("promocodes").arg(code).query_async(&mut conn).await.unwrap_or(None);
                if let Some(json_str) = promo_str {
                    if let Ok(mut promo_data) = serde_json::from_str::<serde_json::Value>(&json_str) {
                        let uses = promo_data["uses"].as_i64().unwrap_or(0);
                        let max_uses = promo_data["max_uses"].as_i64().unwrap_or(0);
                        let days = promo_data["days"].as_i64().unwrap_or(0);

                        if uses >= max_uses {
                            bot.send_message(msg.chat.id, "Этот промокод больше не действителен (лимит исчерпан).").await?;
                            return Ok(());
                        }

                        // Apply promo
                        let key = format!("user_settings:{}", user_id);
                        let mut settings = fsocial_common::UserSettings::default();
                        let res: redis::RedisResult<String> = redis::cmd("GET").arg(&key).query_async(&mut conn).await;
                        if let Ok(val) = res {
                            if let Ok(parsed) = serde_json::from_str::<fsocial_common::UserSettings>(&val) {
                                settings = parsed;
                            }
                        }

                        let promo_type = promo_data["type"].as_str().unwrap_or("days");
                        let mut response_msg = String::new();

                        if promo_type == "discount" {
                            let discount_percent = promo_data["discount_percent"].as_i64().unwrap_or(0);
                            settings.active_discount_percent = discount_percent as u8;
                            response_msg = format!("🎉 Промокод успешно активирован! Ваша скидка {}% применена к будущим покупкам.", discount_percent);
                        } else {
                            let now = chrono::Utc::now().timestamp();
                            let current_until = settings.premium_until.unwrap_or(now).max(now);
                            settings.premium_until = Some(current_until + days * 86400);
                            response_msg = format!("🎉 Промокод успешно активирован! Вы получили Premium на {} дней.", days);
                        }

                        if let Ok(json) = serde_json::to_string(&settings) {
                            let _: redis::RedisResult<()> = redis::cmd("SET").arg(&key).arg(json).query_async(&mut conn).await;
                            
                            // Mark as used
                            let _: redis::RedisResult<()> = redis::cmd("SADD").arg(&used_key).arg(user_id).query_async(&mut conn).await;
                            
                            // Increment uses
                            promo_data["uses"] = serde_json::json!(uses + 1);
                            let _: redis::RedisResult<()> = redis::cmd("HSET").arg("promocodes").arg(code).arg(promo_data.to_string()).query_async(&mut conn).await;

                            bot.send_message(msg.chat.id, response_msg).await?;
                            return Ok(());
                        }
                    }
                }
                
                bot.send_message(msg.chat.id, "Неверный промокод.").await?;
            }
        }
    }
    Ok(())
}
