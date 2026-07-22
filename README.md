# 🚀 FSocial Media Downloader

**Высоконагруженный мультимедийный Telegram-шлюз** для интеллектуальной экстракции медиаконтента из социальных сетей.

## Архитектура

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  Telegram Users  │◄───►│ Local Bot API │◄───►│    Gateway      │
│                  │     │ (2GB files)  │     │ (Rust/Teloxide) │
└─────────────────┘     └──────────────┘     └───────┬─────────┘
                                                      │ NATS
                                              ┌───────┴─────────┐
                                              │   NATS JetStream │
                                              │  (Message Broker)│
                                              └───────┬─────────┘
                                                      │ Queue Groups
                                    ┌─────────────────┼─────────────────┐
                                    ▼                 ▼                 ▼
                             ┌──────────┐     ┌──────────┐     ┌──────────┐
                             │ Worker 1 │     │ Worker 2 │     │ Worker N │
                             │ (yt-dlp) │     │ (yt-dlp) │     │ (yt-dlp) │
                             └──────────┘     └──────────┘     └──────────┘
                                    │                 │                 │
                                    └─────────────────┴─────────────────┘
                                              │ Zero-Copy
                                       ┌──────┴──────┐
                                       │ Shared Vol  │
                                       │ /shared_data│
                                       └─────────────┘
```

## Поддерживаемые платформы

| Платформа | Видео | Аудио | Плейлисты |
|-----------|:-----:|:-----:|:---------:|
| YouTube   | ✅    | ✅    | —         |
| TikTok    | ✅    | —     | —         |
| Instagram | ✅    | —     | —         |
| Spotify   | —     | ✅    | ✅        |
| SoundCloud| —     | ✅    | —         |
| Pinterest | ✅    | —     | —         |

## Технологический стек

- **Rust** + **Tokio** — асинхронный рантайм с work-stealing планировщиком
- **Teloxide** — фреймворк для Telegram Bot API
- **NATS JetStream** — легковесный брокер сообщений (~50MB RAM)
- **yt-dlp** + **ffmpeg** — экстракция и конвертация медиа
- **Redis** — кэширование (FSM + L2 cache)
- **PostgreSQL** — профили пользователей и статистика
- **Docker Compose** — оркестрация микросервисов
- **Local Bot API Server** — лимит файлов до 2 ГБ (вместо 50 МБ)

## Быстрый старт

### 1. Клонирование и настройка

```bash
git clone <repo-url>
cd FSocial_media_downloader
cp .env.example .env
# Заполните .env: TELOXIDE_TOKEN, TELEGRAM_API_ID, TELEGRAM_API_HASH
```

### 2. Запуск

```bash
# Запуск всех сервисов
docker compose up -d

# Масштабирование воркеров (по необходимости)
docker compose up -d --scale worker=4

# Мониторинг логов
docker compose logs -f gateway worker
```

### 3. Настройка Spotify (опционально)

1. Создайте приложение на [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
2. Добавьте `SPOTIFY_CLIENT_ID` и `SPOTIFY_CLIENT_SECRET` в `.env`

### 4. Настройка прокси (опционально)

Для обхода Cloudflare/DataDome добавьте мобильные прокси в `.env`:
```
PROXY_LIST=socks5://user:pass@proxy1:1080,socks5://user:pass@proxy2:1080
```

## Команды бота

| Команда | Описание |
|---------|----------|
| `/start` | Начало работы |
| `/help` | Справка по использованию |
| `/quality` | Настройка качества по умолчанию |
| `/settings` | Текущие настройки |

## Использование

**Личные сообщения:** Отправьте ссылку → Выберите качество → Получите файл

**Групповые чаты:** Просто отправьте ссылку — бот автоматически скачает в 720p и отправит ответом

## Структура проекта

```
FSocial_media_downloader/
├── common/          # Общие типы и конфигурация
│   └── src/
│       ├── models.rs    # DownloadTask, TaskResult, Quality, Platform
│       ├── config.rs    # AppConfig (env vars)
│       └── error.rs     # AppError (unified errors)
├── gateway/         # Telegram Gateway (teloxide + NATS publisher)
│   └── src/
│       ├── main.rs          # Entry point, dispatcher setup
│       ├── commands.rs      # /start, /help, /quality, /settings
│       ├── url_parser.rs    # URL detection & platform identification
│       ├── nats_client.rs   # NATS JetStream publisher
│       ├── nats_listener.rs # Result listener (sends files to Telegram)
│       └── handlers/        # Message & callback handlers
├── worker/          # Media Worker (yt-dlp + Spotify + tagging)
│   └── src/
│       ├── main.rs          # Entry point, NATS consumer
│       ├── nats_consumer.rs # Queue Group consumer
│       ├── media/           # yt-dlp, caching, proxy rotation
│       └── audio/           # Spotify API, YouTube matching, ID3 tagging
├── docker-compose.yml
├── Dockerfile.gateway
├── Dockerfile.worker
├── nats-server.conf
└── migrations/
    └── 001_init.sql
```

## Мониторинг

- **NATS Dashboard:** http://localhost:8222
- **Telegram Bot API Stats:** http://localhost:8082

## Лицензия

MIT
