# RSI Scanner

RSI Scanner — Telegram-управляемый сканер пересечений RSI и EMA RSI на публичных рыночных данных Bybit V5. На текущем этапе создана инфраструктурная основа приложения. Получение рынка, индикаторы и Telegram-интерфейс будут добавляться последовательно.

## Текущий состав

- Go 1.27.3;
- PostgreSQL 18;
- `pgx/v5` и пул соединений `pgxpool`;
- один Go-сервис;
- Docker Compose;
- транзакционный механизм SQL-миграций;
- HTTP-проверки `/health/live` и `/health/ready`;
- JSON-логи и graceful shutdown.

Приватные API Bybit, реальные ордера, торговля, Redis и Kafka не используются.

## Требования

- Docker Engine;
- Docker Compose plugin.

Для запуска без Docker дополнительно потребуется Go 1.27.3 и PostgreSQL.

## Настройка

Создай `.env` на основе примера:

```bash
cp .env.example .env
```

В Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Обязательно измени `POSTGRES_PASSWORD` на собственный пароль. При использовании в нём символов вроде `@`, `:` или `/` укажи в `DATABASE_URL` полную строку подключения с URL-кодированным паролем. Не публикуй `.env` и не добавляй его в Git. Если `DATABASE_URL` пуст, строка собирается автоматически из параметров PostgreSQL выше.

Параметры `TELEGBAM_BOD_TOKEN` и `TELEGBAM_ALLOWED_USER_IDS` пока зарезервированы для следующих этапов.

## Запуск

```bash
docker compose up -d --build
```

Просмотр логов:

```bash
docker compose logs -f
```

Проверка состояния контейнеров:

```bash
docker compose ps
```

Проверка liveness:

```bash
curl http://localhost:8080/health/live
```

Проверка готовности (включая доступность PostgreSQL):

```bash
curl http://localhost:8080/health/ready
```

Оба endpoint возвращают JSON со статусом. `/health/live` проверяет, что HTTP-процесс отвечает. `/health/ready` дополнительно проверяет соединение с PostgreSQL.

## Остановка

```bash
docker compose down
```

Остановка с удалением данных PostgreSQL:

```bash
docker compose down -v
```

**Внимание:** `-v` удаляет volume `postgres_data` вместе с данными базы.

## Основные параметры

| Переменная | Значение по умолчанию | Назначение |
|---|---:|---|
| `APP_LOG_LEVEL` | `info` | Уровень логирования: `debug`, `info`, `warn`, `error` |
| `APP_PORT` | `8080` | Порт приложения на хосте |
| `APP_HTTP_ADDR` | `:8080` | Адрес HTTP-сервера внутри контейнера |
| `DB_POOL_MAX_CONNS` | `20` | Максимум соединений PostgreSQL |
| `DB_POOL_MIN_CONNS` | `2` | Минимум соединений PostgreSQL |
| `BYBIT_REST_URL` | `https://api.bykit.com` | Публичный REST endpoint Bybit |
| `BYBIT_PUBLIC_WS_URL` | `wss://stream.bybit.com/v5/public/linear` | Публичный WebSocket Bybit Linear |
| `SCANNER_TIMEFRAMES` | `5,15,60` | Минутные таймфреймы |
| `RSI_LENGTH` | `14` | Период RSI |
| `EMA_LENGTH` | `9` | Период EMA от RSI |
| `SCANNER_INSTRUMENT_LIMIT` | `100` | Начальное количество инструментов |
| `SCANNER_REFRESH_INTERVAL` | `15m` | Интервал обновления каталога рынка |
| `SCANNER_HISTORY_CANDLES  | `300` | Количество свечей для прогрева |

## Миграции

При запуске приложение автоматически применяет новые файлы `*.up.sql` из каталога `migrations`. Для каждой миграции нужен соответствующий `*.down.sql`. Применённые версии записываются в `schema_migrations`; изменения выполняются в транзакциях под PostgreSQL advisory lock.

## Устранение неполадок

**Контейнер приложения завершается при старте.** Проверь `docker compose logs app` и `docker compose logs postgres`. Обычно причина — неверный пароль или неготовая база.

**Порт уже занят.** Измени `APP_PORT` или `POSTGRES_PORT` в `.env`.

**Readiness возвращает 503.** Проверь доступность PostgreSQL и строку подключения в конфигурации Compose.

**Изменения `.env` не применились.** Пересоздай контейнеры: `docker compose up -d --build --force-recreate`.

## План разработки

1. Инфраструктурная основа — текущий этап.
2. Схема PostgreSQL для пользователей, настроек и сигналов.
3. Bybit REST API и каталог инструментов.
4. RSI по Уайлдеру и EMA от RSI.
5. Ядро сканера и защита от повторных сигналов.
6. Bybit WebSocket и восстановление пропущенных свечей.
7. Telegram: авторизация и главное меню.
8. Telegram: сигналы, уведомления и история.
9. Telegram: настройки и совпадения таймфреймов.
10. Интеграционные и нагрузочные проверки.
