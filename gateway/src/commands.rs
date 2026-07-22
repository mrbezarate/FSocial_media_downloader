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

pub async fn handle(bot: Bot, msg: Message, cmd: Command) -> ResponseResult<()> {
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
                        В группах я скачиваю медиа автоматически в качестве по умолчанию (720p / 256kbps).";
            bot.send_message(msg.chat.id, text).await?;
        }
        Command::Quality => {
            let text = "⚙️ Текущее качество по умолчанию (для групп):\n\n\
                        📹 Видео: 720p\n\
                        🎵 Аудио: 256 kbps";
            bot.send_message(msg.chat.id, text).await?;
        }
        Command::Settings => {
            let text = "🔧 Настройки:\n\n\
                        • Автоматическое скачивание в группах: Включено\n\
                        • Выбор качества в ЛС: Включено";
            bot.send_message(msg.chat.id, text).await?;
        }
    }
    Ok(())
}
