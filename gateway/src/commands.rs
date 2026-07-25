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
    }
    Ok(())
}
