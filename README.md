<div align="center">
  <h1>🚀 FSocial Media Downloader</h1>
  <p><strong>Высоконагруженный мультимедийный Telegram-шлюз на Rust</strong></p>
  
  <p>
    <a href="https://t.me/FSocial_Media_Downloader_bot">
      <img src="https://img.shields.io/badge/Telegram-Try_Bot-2CA5E0?style=for-the-badge&logo=telegram" alt="Telegram Bot" />
    </a>
  </p>
  
  <p>
    <img src="https://img.shields.io/badge/Language-Rust-f74c00?style=for-the-badge&logo=rust" alt="Rust" />
    <img src="https://img.shields.io/badge/Broker-NATS-27A1E1?style=for-the-badge&logo=nats" alt="NATS JetStream" />
    <img src="https://img.shields.io/badge/Framework-Teloxide-0088cc?style=for-the-badge&logo=telegram" alt="Teloxide" />
    <img src="https://img.shields.io/badge/Database-PostgreSQL-316192?style=for-the-badge&logo=postgresql" alt="Postgres" />
    <img src="https://img.shields.io/badge/Cache-Redis-DC382D?style=for-the-badge&logo=redis" alt="Redis" />
    <img src="https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker" alt="Docker" />
  </p>
</div>

---

**FSocial Media Downloader** — это продвинутый микросервисный Telegram-бот, способный выдерживать огромные нагрузки и мгновенно скачивать медиаконтент из популярных социальных сетей. Архитектура построена на `NATS JetStream` и распределённых воркерах, что позволяет масштабировать бота до бесконечности!

## ✨ Ключевые фичи

- **Индивидуальные настройки**: Профили пользователей хранятся в Redis. Команда `/settings` позволяет настроить дефолтное качество для авто-скачивания и включить "Тихий режим".
- **Premium Подписка (Telegram Stars)**: Встроенная монетизация и разделение на `free` и `premium` очереди воркеров. Премиум-пользователи получают максимальный приоритет и отсутствие лимитов на плейлисты.
- **Inline-режим (@бот url)**: Мгновенный шеринг контента. Введите имя бота и ссылку в любом чате, чтобы моментально получить файл из кэша.
- **Smart Fallback & Лимиты**: Умная система понижения качества. Если видео не влезает в лимит Telegram (50 МБ для облака или 1 ГБ для локального сервера), бот *автоматически* скачает версию полегче без ошибок и сбоев!
- **Файлы до 1 ГБ**: Поддержка огромных файлов через Local Telegram Bot API Server!
- **Безумная масштабируемость**: Gateway и Worker разделены. Вы можете запустить хоть 100 воркеров одной командой `docker compose up --scale worker=100`.
- **Поддержка Плейлистов**: Способен асинхронно и параллельно скачивать целые альбомы и плейлисты из Spotify/SoundCloud.
- **Умное кэширование**: Повторные запросы одних и тех же ссылок отдаются из базы Redis за миллисекунды.
- **Интеграция со Spotify**: Подхватывает метаданные (название, автор, год) и аккуратно вшивает их (ID3 tags) в MP3 файл.
- **Вшивание обложек**: Telegram API строго относится к миниатюрам. Бот автоматически сжимает и накладывает обложки (`ffmpeg scale=320:320`) прямо поверх аудио/видео.

## 📱 Поддерживаемые платформы

| Платформа | Одиночное Видео / Reels | Трек (Аудио) | Плейлисты / Альбомы |
|:---|:---:|:---:|:---:|
| 📺 **YouTube** | ✅ (до 4K) | ✅ (MP3) | ❌ |
| 🎵 **Spotify** | ❌ | ✅ (MP3 + ID3 Обложка) | ✅ |
| ☁️ **SoundCloud** | ❌ | ✅ (MP3) | ✅ |
| 📱 **TikTok** | ✅ (Без вотермарок) | ❌ | ❌ |
| 📸 **Instagram** | ✅ (Reels / Posts) | ❌ | ❌ |
| 📌 **Pinterest** | ✅ | ❌ | ❌ |

## 🏗 Архитектура

```mermaid
graph TD
    User([👤 Telegram User]) <-->|Message| TG(Local Bot API Server\n<2GB limits>)
    TG <-->|Webhook / Polling| Gateway(Rust Gateway\nTeloxide)
    
    Gateway -->|Publish DownloadTask| NATS{NATS JetStream\nBroker}
    NATS -->|TaskStatus & Progress| Gateway
    
    NATS -->|Free Queue| WF1(Free Worker 1)
    NATS -->|Free Queue| WFN(Free Worker N)
    NATS -->|Premium Queue| WP1(Premium Worker 1)
    
    WF1 -.->|Zero Copy write| SharedVol[(Shared Docker Volume\n/shared_data)]
    WFN -.->|Zero Copy write| SharedVol
    WP1 -.->|Zero Copy write| SharedVol
    SharedVol -.->|Zero Copy read| TG
    
    WF1 <--> Redis[(Redis Cache)]
    Gateway <--> PG[(PostgreSQL\nProfiles)]
```

## 🛠 Технологический стек

- 🦀 **Rust** + **Tokio** — для максимальной скорости и безопасности работы с памятью.
- 💬 **Teloxide** — лучший асинхронный фреймворк для Telegram ботов на Rust.
- 🚀 **NATS JetStream** — высокопроизводительный брокер сообщений (жрёт всего ~50 МБ RAM).
- 💿 **PostgreSQL** & **Redis** — для состояний стейт-машины (FSM), статистики и L2 кэширования медиа.
- 🎬 **yt-dlp** + **ffmpeg** + **lofty** — ядро загрузки, кодирования и простановки ID3v2 тегов.
- 🐳 **Docker Compose** — для безболезненного деплоя всей инфраструктуры в одну команду.

---

## 🚀 Быстрый старт (Deployment)

Вам потребуется сервер с установленным `Docker` и `Docker Compose`.

### 1. Клонирование и настройка
```bash
git clone https://github.com/your-username/FSocial_media_downloader.git
cd FSocial_media_downloader

# Создайте файл с переменными окружения
cp .env.example .env
```

Отредактируйте `.env`, вписав необходимые данные:
```env
TELOXIDE_TOKEN=your_bot_token_here
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
POSTGRES_PASSWORD=secure_database_password
```

### 2. Запуск инфраструктуры
```bash
# Запуск Gateway, 1x Free Worker, 1x Premium Worker, NATS, Redis, Postgres, Bot API Server
docker compose up -d --build

# Мгновенное масштабирование для разных тиров пользователей!
docker compose up -d --scale worker=4 --scale pre-worker=2
```

### 3. Наблюдение за полётом
```bash
# Мониторинг логов
docker compose logs -f gateway worker
```
- **NATS Dashboard:** `http://localhost:8222`
- **Telegram API Stats:** `http://localhost:8082`

## 🎧 Настройка парсинга Spotify (Опционально)
По умолчанию бот может парсить Spotify через веб-скрапинг. Для большей надёжности:
1. Зайдите на [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
2. Создайте приложение и скопируйте ключи.
3. Добавьте в `.env`:
   ```env
   SPOTIFY_CLIENT_ID=your_client_id
   SPOTIFY_CLIENT_SECRET=your_client_secret
   ```

## 🛡 Обход блокировок (Прокси)
Если сервер блокируется на YouTube или DataDome:
```env
PROXY_LIST=socks5://user:pass@1.1.1.1:1080,socks5://user:pass@2.2.2.2:1080
```
Воркеры будут автоматически ротировать прокси-сервера.

---
## 📜 Команды
- `/start` — 🚀 Запустить бота / Помощь
- `/settings` — ⚙️ Настройки качества и звука (Ваши текущие настройки и статистика профиля)
- `/premium` — 💎 Купить подписку Premium за Telegram Stars
- `/admin` — 👑 Админ-команды (например, выдача премиума: `/admin give_premium <user_id> <days>`)
- `/help` — ❓ Как пользоваться ботом

---
<div align="center">
  <i>Разработано с любовью 💖.</i>
</div>

### 🍪 Работа с cookies (yt-dlp)
Если для некоторых сервисов (например, Instagram, Spotify) требуется авторизация, вы можете передать файл `cookies.txt` в `yt-dlp`. 
1. Сгенерируйте `cookies.txt` в браузере (например, расширением "Get cookies.txt").
2. Положите файл в надежное место (и убедитесь, что он добавлен в `.gitignore` во избежание утечки!).
3. В файле `.env` укажите путь: `COOKIES_PATH=/app/cookies.txt` или локальный путь.
