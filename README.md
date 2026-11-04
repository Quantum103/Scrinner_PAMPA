# RSI Scanner

RSI Scanner — Telegram-управляемый сканер пересечений RSI и EMA RSI на публичных рыночных данных Bybit V5. Проект развивается поэтапно; текущая версия содержит инфраструктуру и PostgreSQL-хранилище.

## Текущий состав

- Go 1.27.3;
- PostgreSQL 18;
- `pgx/v5` и пул соединений `pgxpool`;
- один Go-сервис;
- Docker Compose;
- транзакционные SQL-миграции;
- хранилище пользователей, настроек, инструментов, сигналов и очереди уведомлений;
- атомарный курсор сканирования для каждой пары «инструмент + таймфрейм»;
- HTTP-проверки `/health/live` и `/health/ready`;
- JSON-логи и graceful shutdown.

Приватные API Bybit, реальные ордера, торговля, Redis и Kafka не используются.

## Схема PostgreSQL

Миграция `000001_initial_schema` создаёт:

| Таблица | Назначение |
|---|---|
| `telegram_users` | Telegram-пользователи, уникальный Telegram ID, активность и даты |
| `user_settings` | Индивидуальные параметры RSI/EMA, количество активов и флаги уведомлений |
| `user_timeframes` | Включённые таймфреймы каждого пользователя |
| `instruments` | Каталог Bybit USDT Linear Perpetual и оборот для сортировки по ликвидности |
| `user_instrument_preferences` | Ручные включения и исключения инструментов |
| `signals` | Глобальные рыночные события пересечения RSI/EMA |
| `scanner_states` | Последняя обработанная закрытая свеча и последние значения индикаторов |
| `signal_deliveries` | Персональная доставка сигнала, попытки, lease и ошибки |

Сигнал не дублируется для разных пользователей: уникальность обеспечивается ограничением PostgreSQL по `symbol + timeframe + candle_start + direction`.

Связь «сигнал → пользователь» хранится отдельно в `signal_deliveries`. У одного рыночного события может быть несколько персональных записей доставки. История пользователя строится через эту связь.

`CommitClosedCandle` в одной транзакции продвигает курсор и записывает сигнал. Повторная или устаревшая свеча не может продвинуть курсор назад и не создаёт новый сигнал. `InitializeScannerState` не перезаписывает существующий курсор при перезапуске.

Очередь доставки поддерживает атомарный захват через `FOR UPDATE SKIP LOCKED`, аренду обработки, повторные попытки и фиксацию статуса отправки. `EnqueueSignalForEligibleUsers` учитывает включённый таймфрейм, LONG/SHORT-переключатель, индивидуальный лимит топа по обороту, режим «все инструменты» и ручные исключения/включения.

## Требования

- Docker Engine;
- Docker Compose plugin.

Для запуска без Docker дополнительно потребуется Go 1.27.3 и PostgreSQL 18.

## Настройка

Создай `.env` на основе примера:

```bash
cp .env.example .env
```

В Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Обязательно измени `POSTGRES_PASSWORD` на собственный пароль. Если пароль содержит URL-спецсимволы вроде `@`, `:` или `/`, укажи в `DATABASE_URL` полную строку подключения с URL-кодированным паролем. Не публикуй `.env` и не добавляй его в Git. Если `DATABASE_URL` пуст, строка подключения строится из параметров PostgreSQL в Compose.

`TELEGRAM_BOT_TOKEN` и `TELEGRAM_ALLOWED_USER_IDS` зарезервированы для Telegram-этапа; Telegram-интерфейс пока не подключён.

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

`/health/live` проверяет, что HTTP-процесс отвечает. `/health/ready` дополнительно проверяет соединение с PostgreSQL.

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
| `BYBIT_REST_URL` | `https://api.bybit.com` | Публичный REST endpoint Bybit |
| `BYBIT_PUBLIC_WS_URL` | `wss://stream.bybit.com/v5/public/linear` | Публичный WebSocket Bybit Linear |
| `SCANNER_TIMEFRAMES` | `5,15,60` | Минутные таймфреймы |
| `RSI_LENGTH` | `14` | Период RSI |
| `EMA_LENGTH` | `9` | Период EMA от RSI |
| `SCANNER_INSTRUMENT_LIMIT` | `100` | Начальное количество инструментов |
| `SCANNER_REFRESH_INTERVAL` | `15m` | Интервал обновления каталога рынка |
| `SCANNER_HISTORY_CANDLES` | `300` | Количество свечей для прогрева |

## Миграции

При старте приложение автоматически применяет новые файлы `*.up.sql` из каталога `migrations`. Для каждой миграции нужен соответствующий `*.down.sql`. Применённые версии записываются в `schema_migrations`; SQL выполняется в транзакциях под PostgreSQL advisory lock.

Миграции не запускаются параллельно несколькими экземплярами приложения: advisory lock сериализует применение схемы.

## Тестирование

Обычный локальный запуск unit-тестов:

```bash
go test ./...
```

PostgreSQL integration-тесты запускаются, если задана переменная `DATABASE_TEST_URL`. Они проверяют миграции, пользовательские настройки, изоляцию истории, каталог инструментов, уникальность сигнала, очередь доставки, повторные попытки и атомарный курсор сканирования.

GitHub Actions поднимает отдельный PostgreSQL 18 service container и выполняет:

- `gofmt`;
- `go test ./...` (unit + PostgreSQL integration);
- `go test -race ./...`;
- `go vet ./...`;
- `go build ./...`.

## Устранение неполадок

**Контейнер приложения завершается при старте.** Проверь `docker compose logs app` и `docker compose logs postgres`. Обычно причина — неверный пароль или неготовая база.

**Порт уже занят.** Измени `APP_PORT` или `POSTGRES_PORT` в `.env`.

**Readiness возвращает 503.** Проверь доступность PostgreSQL и строку подключения в конфигурации Compose.

**Изменения `.env` не применились.** Пересоздай контейнеры: `docker compose up -d --build --force-recreate`.

## Следующие этапы

1. Bybit REST API и каталог инструментов.
2. RSI по Уайлдеру и EMA от RSI.
3. Ядро сканера и защита от повторных сигналов.
4. Bybit WebSocket и восстановление пропущенных свечей.
5. Telegram: авторизация, меню, сигналы, уведомления, история и настройки.
6. Интеграционные и нагрузочные проверки всего приложения.
