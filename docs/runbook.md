# Локальный runbook

Команды выполняются из корня LedgerCore и относятся только к этому Compose-проекту. HTTP Gateway: `127.0.0.1:18080/18081`; метрики: `19090/19091`. Внутренние Ledger `50051`, Activity `50052`, PostgreSQL/Kafka/Redis доступны только внутри Docker-сети. Настройки — [.env.example](../.env.example); plaintext и демонстрационные credentials предназначены для локального доверенного контура.

## Запуск, остановка, состояние

```sh
docker compose up --build -d --wait --wait-timeout 180
docker compose ps -a
docker compose logs --no-color --tail 80 ledger publisher activity gateway gateway2
docker compose --profile tools run --rm --no-deps ledgerctl events-demo --phase status
```

`make down` сохраняет PostgreSQL, Kafka и Redis volumes. `make destroy-data` удаляет **все** постоянные тома проекта; применять только для явно ненужных локальных тестовых данных. Смена пароля в `.env` не меняет пароль роли в существующем PostgreSQL volume: ротация требует явного ALTER ROLE и согласованной смены конфигурации.

| Проверка | Значение |
| --- | --- |
| Gateway `/livez` | HTTP listener отвечает |
| Gateway `/readyz` | Ledger health + Redis доступны, timeout 1 s; Activity не блокирует прием финансовых команд |
| Ledger gRPC Health Check | Собственная БД доступна |
| Publisher `/readyz` на внутреннем `9092` | БД outbox и Kafka доступны |
| Activity `/readyz` на внутреннем `9093` | Собственная БД и Kafka доступны; нулевой lag не обещается |

Не публикуйте внутренние write/RPC-порты для обхода Gateway. Profile `compat` сохраняет старый HTTP внутри сети и использует те же финансовые сценарии.

Если Gateway отвечает медленно при здоровом Ledger, проверьте сетевое разрешение имени, соединения и фактическую версию образа. В текущем gRPC-клиенте `WithDisableServiceConfig` отключает ненужный DNS TXT lookup: раньше он мог задерживать подключение к Docker service name после успешного A lookup из-за внешнего DNS timeout. Политика retry/deadline задается кодом клиента; после обновления транспортного кода пересоберите приложения командой запуска выше.

## Неизвестный исход команды

`503`, deadline и потерянный ответ не доказывают rollback. Сохраните исходные `Idempotency-Key` и параметры; повторите их либо запросите известный `GET /v1/transfers/{id}`. Новый ключ означает новую операцию. При `429` дождитесь `Retry-After`; при `503 rate_limiter_unavailable` сначала восстановите Redis. Unkeyed `POST /v1/accounts` не имеет этой гарантии автоматического повтора.

## Kafka outage и растущий outbox

Проверьте `ledger_outbox_backlog`, `ledger_outbox_oldest_seconds`, `ledger_outbox_publications_total`/`ledger_outbox_retries_total`, logs и место PostgreSQL/Kafka. Состояние через CLI доступно без зависимостей `ledgerctl` благодаря `--no-deps` выше. Ledger продолжает commit, пока его PostgreSQL работоспособен; event rows не удаляются из-за outage.

```sh
docker compose up -d --no-deps --wait --wait-timeout 180 kafka
docker compose up -d --no-deps --wait --wait-timeout 60 publisher activity
docker compose --profile tools run --rm --no-deps ledgerctl events-demo --phase verify
```

Persistent fetch/SQL/offset/DLQ ошибки останавливают Activity; Compose не задает автоматическую restart policy. После ремонта зависимости явно запустите Activity снова. У publisher истекшие leases возобновляются с теми же IDs; default lease 30 s, поэтому мгновенный нулевой backlog после restart не гарантирован. Повторная Kafka-доставка ожидаема и безопасна для inbox.

Состояние группы и partitions при необходимости:

```sh
docker compose exec -e KAFKA_HEAP_OPTS=-Xmx96m kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group activity-v1
docker compose exec -e KAFKA_HEAP_OPTS=-Xmx96m kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic ledger.operations.v1
```

Имена topic/group могут быть переопределены в `.env`. В dev RF=1/minISR=1; единственный диск/узел не резервирован. Retention 7 дней/256 MiB на partition ограничивает доступный диапазон. Архива нет. Не удаляйте outbox/inbox для «лечения» lag и не делайте повторные переводы вместо утраченной истории. Опубликованные outbox и inbox тоже растут: автоматической очистки нет.

## Activity отсутствует или отстает

Проверьте `activity_consumer_lag`, `activity_processed_total`, `activity_duplicates_total`, `activity_active_partitions`, DB pool и `activity_visibility_seconds`. Lag — последняя наблюденная разница high watermark/processed offset; остановленный consumer может оставлять старое значение. Visibility histogram начинается с pre-commit `occurred_at`, не является точной commit latency. Проверьте actual group offsets командой выше.

Сбой БД приводит к ограниченным попыткам и остановке consumer без пропуска offset. После исправления БД перезапустите Activity. Пустой ответ Activity допустим для неизвестного/еще не спроецированного счета; наличие и средства проверяйте через Ledger. Позднее событие появляется в новой пагинации, а не вставляется в уже закрепленный cursor prefix.

## DLQ

Ищите outcome `dlq`/`dlq_error` в `activity_processed_total`. Envelope содержит классификацию, `topic/partition/offset`, hash/длину и ограниченный payload; в logs тело не печатается. Сначала выясните, почему вход несовместим или конфликтует с существующим ID. Успешная DLQ-доставка разрешает commit исходного offset; отказ DLQ — нет. DLQ может повторяться со стабильным source ID.

Не редактируйте существующий inbox/event ID и не продвигайте offsets вручную, чтобы скрыть ошибку. Исправление несовместимой схемы, повторная публикация исправленного сообщения и исторический backfill требуют отдельного явного плана с сохранением исходной идентичности/аудита.

## Redis outage/лимит

`gateway_rate_admissions_total{outcome="limited"}` означает 429, `outcome="error"` — 503. Общий лимит должен иметь одинаковые `API_TOKEN`, `RATE_LIMIT`, `RATE_WINDOW` на обоих Gateway. Redis admission timeout 300 ms; запись в Ledger не вызывается при ошибке.

```sh
docker compose up -d --no-deps --wait --wait-timeout 60 redis
```

Не сбрасывайте Redis целиком ради 429: это сбросит бюджет клиента. Дождитесь TTL. Потеря Redis/AOF может начать новое окно; это не влияет на durable financial idempotency. Fixed-window-подобный алгоритм допускает всплеск около границы двух окон.

## Replay и сверки

Остановите внешнюю нагрузку, дождитесь восстановления зависимостей. Для повторного чтения сохраненного диапазона с новым group ID в существующую проекцию:

```sh
docker compose --profile tools run --rm --no-deps ledgerctl events-demo --phase replay
docker compose --profile tools run --rm --no-deps ledgerctl events-demo --phase verify
make reconcile
```

Replay сохраняет inbox/entries/NUMERIC до/после; новые deliveries не означают новые бизнес-операции. Не удаляйте inbox раньше допустимого replay-горизонта; очистка в этой версии не реализована. Эти команды не обещают rebuild уже истекшей неархивированной Kafka-истории.

Финансовая сверка проверяет один REPEATABLE READ snapshot независимо от writer. Сверка проекции заново выводит entries/aggregates и сравнивает множества journal/outbox/inbox и payload. Cross-DB полнота требует quiescent writers; распределенного snapshot нет. Ненулевой exit code требует расследования, а не автоматического «исправления» balance/summary. Результаты и raw отчеты — [приемка](events/acceptance.md), [demo](demo.md), [измерения](benchmarks.md).
