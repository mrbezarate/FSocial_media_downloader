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
}

pub async fn handle(bot: crate::MyBot, msg: Message, cmd: Command, redis_pool: deadpool_redis::Pool) -> ResponseResult<()> {
    match cmd {
        Command::Start => {
            let text = "👋 Привет! Я бот для загрузки медиа.\n\n\
                        Поддерживаемые платформы:\n\
                        - YouTube\n\
                        - TikTok\n\
                        - Instagram\n\
                        - Spotify\n\
                        - SoundCloud\n\
                        - Pinterest\n\n\
                        Просто отправь мне ссылку, и я скачаю её для тебя!";
            bot.send_message(msg.chat.id, text).await?;
        }
        Command::Help => {
            let text = "📖 Как использовать:\n\n\
                        1. Отправь ссылку на видео/аудио в личные сообщения.\n\
                        2. Выбери качество из появившегося меню.\n\
                        3. Дождись окончания загрузки.\n\n\
                        В группах я скачиваю медиа автоматически в качестве по умолчанию (настраивается в /settings).";
            bot.send_message(msg.chat.id, text).await?;
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
                let res: redis::RedisResult<String> = redis::cmd("GET").arg(&key).query_async(&mut conn).await;
                if let Ok(val) = res {
                    if let Ok(parsed) = serde_json::from_str::<fsocial_common::UserSettings>(&val) {
                        settings = parsed;
                    }
                }
            }

            let text = "🔧 <b>Настройки профиля</b>\n\nЗдесь вы можете выбрать качество по умолчанию для загрузок в группах (или быстрых загрузок).";
            
            use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};
            
            let vid_text = format!("📹 Видео: {:?}", settings.default_video);
            let aud_text = format!("🎵 Аудио: {:?}", settings.default_audio);
            let quiet_text = format!("🤫 Тихий режим: {}", if settings.quiet_mode { "ВКЛ" } else { "ВЫКЛ" });

            let keyboard = InlineKeyboardMarkup::new(vec![
                vec![InlineKeyboardButton::callback(vid_text, "setmenu|vid")],
                vec![InlineKeyboardButton::callback(aud_text, "setmenu|aud")],
                vec![InlineKeyboardButton::callback(quiet_text, "set_quiet")],
            ]);

            bot.send_message(msg.chat.id, text)
                .parse_mode(teloxide::types::ParseMode::Html)
                .reply_markup(keyboard)
                .await?;
        }
        Command::Premium => {
            let title = "Premium Подписка 💎";
            let description = "Месяц безграничных загрузок: неограниченный трафик, плейлисты любой длины и максимальный приоритет в очереди!";
            let payload = "premium_1_month";
            let provider_token = ""; // Оставляем пустым для Telegram Stars
            let currency = "XTR"; // Telegram Stars
            let prices = vec![teloxide::types::LabeledPrice {
                label: "1 Месяц Premium".into(),
                amount: 500, // 500 звезд
            }];

            bot.send_invoice(msg.chat.id, title, description, payload, currency, prices)
                .await?;
        }
        Command::Admin(args) => {
            let admin_id_str = std::env::var("ADMIN_ID").unwrap_or_default();
            let admin_id: u64 = admin_id_str.parse().unwrap_or(0);
            
            let user_id = msg.from.as_ref().map(|u| u.id.0).unwrap_or(0);
            if user_id == 0 || user_id != admin_id {
                return Ok(());
            }

            let parts: Vec<&str> = args.split_whitespace().collect();
            if parts.is_empty() {
                bot.send_message(msg.chat.id, "Использование:\n/admin give_premium <user_id> <days>").await?;
                return Ok(());
            }

            if parts[0] == "give_premium" && parts.len() == 3 {
                if let (Ok(target_id), Ok(days)) = (parts[1].parse::<u64>(), parts[2].parse::<i64>()) {
                    if let Ok(mut conn) = redis_pool.get().await {
                        let key = format!("user_settings:{}", target_id);
                        let mut target_settings = fsocial_common::UserSettings::default();
                        
                        let res: redis::RedisResult<String> = redis::cmd("GET").arg(&key).query_async(&mut conn).await;
                        if let Ok(val) = res {
                            if let Ok(parsed) = serde_json::from_str::<fsocial_common::UserSettings>(&val) {
                                target_settings = parsed;
                            }
                        }
                        
                        let now = chrono::Utc::now().timestamp();
                        let current_until = target_settings.premium_until.unwrap_or(now).max(now);
                        target_settings.premium_until = Some(current_until + days * 86400);
                        
                        if let Ok(json) = serde_json::to_string(&target_settings) {
                            let _: redis::RedisResult<()> = redis::cmd("SET").arg(&key).arg(json).query_async(&mut conn).await;
                            bot.send_message(msg.chat.id, format!("Успешно выдано {} дней Premium пользователю {}", days, target_id)).await?;
                            let _ = bot.send_message(teloxide::types::ChatId(target_id as i64), format!("🎉 Вам был выдан Premium на {} дней!", days)).await;
                            return Ok(());
                        }
                    }
                }
            }
            
            bot.send_message(msg.chat.id, "Команда не распознана или неверные аргументы.").await?;
        }
    }
    Ok(())
}
