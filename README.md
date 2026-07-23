<div align="center">
  <h1>🚀 FSocial Media Downloader</h1>
  <p><strong>Высоконагруженный мультимедийный Telegram-шлюз на Rust</strong></p>
  
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

- **Файлы до 2 ГБ**: Благодаря встроенному `Local Telegram Bot API Server`, бот обходит стандартный лимит Telegram (50 МБ) и скачивает 4K видео!
- **Безумная масштабируемость**: Gateway и Worker разделены. Вы можете запустить хоть 100 воркеров одной командой `docker compose up --scale worker=100`.
- **Zero-Copy Трансфер**: Воркеры качают файлы в `/shared_data`, а Gateway моментально отдает их в Telegram через Local Bot API без лишнего копирования по сети.
- **Поддержка Плейлистов**: Способен скачивать целые альбомы и плейлисты из Spotify/SoundCloud с кнопкой «Отмена/Пауза» в реальном времени.
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
    
    NATS -->|Queue Group / Pull| W1(Worker 1)
    NATS -->|Queue Group / Pull| W2(Worker 2)
    NATS -->|Queue Group / Pull| WN(Worker N)
    
    W1 -.->|Zero Copy write| SharedVol[(Shared Docker Volume\n/shared_data)]
    W2 -.->|Zero Copy write| SharedVol
    WN -.->|Zero Copy write| SharedVol
    SharedVol -.->|Zero Copy read| TG
    
    W1 <--> Redis[(Redis Cache)]
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
# Запуск Gateway, 1x Worker, NATS, Redis, Postgres, Bot API Server
docker compose up -d --build

# Если нагрузка растёт, мгновенно добавляем воркеры!
docker compose up -d --scale worker=4
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

## 🕹 Использование бота

| Фича | Как работает |
|:---|:---|
| **Личные сообщения** | Пришли ссылку боту. Он покажет красивые UI-кнопки (инлайн) с выбором формата и качества. Нажми кнопку — скачивание начнётся. |
| **Групповые чаты** | Добавь бота в группу. Любая отправленная ссылка будет моментально скачана в лучшем/стандартном качестве (720p) и отправлена как ответ! Никаких лишних действий. |
| **Прерывание скачивания** | Запустил плейлист из 100 треков и передумал? Прямо под сообщением появится кнопка `Отменить (Пауза)`! |

## 📜 Команды
- `/start` — Инициализация и меню
- `/help` — Справка
- `/quality` — Настройка качества видео по умолчанию
- `/settings` — Ваши текущие настройки и статистика профиля

---
<div align="center">
  <i>Разработано с любовью 💖. Лицензия MIT.</i>
</div>
