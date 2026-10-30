# LedgerCore

[![verify](https://github.com/nabiatakhanov/ledgercore/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/nabiatakhanov/ledgercore/actions/workflows/ci.yml)

Учебный Go-сервис внутренних переводов в RUB с событийной историей. Это развитие одного LedgerCore: исходная двойная запись, неизменяемый журнал, идемпотентность, полная отмена и независимая сверка дополнены gRPC, Kafka и Redis. Используются тестовые средства.

| Компонент | Назначение |
| --- | --- |
| Gateway ×2 | HTTP API, реальные gRPC-вызовы LedgerCore/Activity, общий Redis rate limiter |
| LedgerCore + PostgreSQL | Источник истины о деньгах; перевод и outbox фиксируются одной SQL-транзакцией |
| Publisher | Доставка зафиксированного outbox в Kafka после освобождения SQL-блокировок |
| Kafka | Асинхронная доставка событий с возможными повторами |
| Activity + отдельная PostgreSQL БД | Inbox, история и статистика; чтение через gRPC, eventually consistent |
| Redis | Общий бюджет write-запросов двух Gateway; денежные данные в нем не хранятся |

## Запуск и демонстрация

Нужны Docker Engine и Compose v2. Из корня репозитория:

```sh
docker compose up --build -d --wait --wait-timeout 180
```

Команда собирает non-root Go-приложения, создает отдельные БД/роли, применяет миграции и поднимает два Gateway. HTTP: [127.0.0.1:18080](http://127.0.0.1:18080/livez) и [127.0.0.1:18081](http://127.0.0.1:18081/livez); метрики: [19090/metrics](http://127.0.0.1:19090/metrics) и [19091/metrics](http://127.0.0.1:19091/metrics). `/livez` и `/readyz` доступны без токена. PostgreSQL, Kafka, Redis и внутренние gRPC-порты наружу не публикуются.

Compose содержит явные локальные демонстрационные пароли и токены из [.env.example](.env.example). Перед использованием общего окружения задайте независимые значения. `API_TOKEN` аутентифицирует HTTP, отдельный `INTERNAL_RPC_TOKEN` — внутренние RPC. Plaintext gRPC и Kafka рассчитаны на приватную локальную Docker-сеть. Статический bearer дает доступ ко всем тестовым кошелькам; разграничения пользователей нет.

```sh
make up                # сборка и запуск
make demo              # перевод, Activity, остановки Kafka/Activity/Redis, replay, лимит, SIGTERM и две сверки
make reconcile         # финансовая сверка и независимая сверка событий/проекции
make bench             # измерение пути Gateway → gRPC → SQL
make down              # остановка с сохранением постоянных данных
```

`make demo` запускает [скрипт Windows](scripts/demo-events.ps1) или [Linux/macOS](scripts/demo-events.sh), сохраняет отчеты в `.runtime/events-demo/` и восстанавливает остановленные компоненты. Полный сценарий использует лимит `60/1m`; ожидание TTL учитывается. Повтор не финансирует те же счета заново: fixtures и обычные операции используют постоянные ключи. Аварийные пакеты имеют отдельный run ID и создают новые явно учтенные тестовые переводы. Команда `make destroy-data` отдельно и явно удаляет все тома этого Compose-проекта.

Без Make: `powershell -File scripts/demo-events.ps1` на Windows или `sh scripts/demo-events.sh` при наличии Python 3 на Unix. [Пошаговая демонстрация](docs/demo.md), [runbook](docs/runbook.md).

## API и гарантии

Исходный [HTTP-контракт](docs/openapi.yaml) сохранен через общий `internal/store`. Рекомендуемая точка входа — Gateway; старый `cmd/api` доступен только во внутреннем Compose profile `compat`.

| Метод | Путь | Назначение |
| --- | --- | --- |
| POST | `/v1/accounts` | Нулевой клиентский кошелек; команда без идемпотентности |
| GET | `/v1/accounts/{id}` | Авторитетный баланс и метаданные LedgerCore |
| POST | `/v1/transfers` | Перевод с обязательным `Idempotency-Key` |
| GET | `/v1/transfers/{id}` | Операция и проводки |
| POST | `/v1/transfers/{id}/reversal` | Полная отмена с отдельным постоянным ключом |
| GET | `/v1/accounts/{id}/entries` | Исходная история LedgerCore |
| GET | `/v1/accounts/{id}/activity` | История и статистика Activity, `limit=1..100`, `cursor` |

Для `/v1` нужен `Authorization: Bearer <API_TOKEN>`. Пример тела перевода:

```json
{
  "from_account_id": "<UUID отправителя>",
  "to_account_id": "<UUID получателя>",
  "amount_minor": "2500",
  "currency": "RUB",
  "metadata": {"purpose": "demo"}
}
```

Деньги передаются HTTP-строками в копейках, внутри операции — проверенный `int64`/SQL `BIGINT`, в Protobuf — `int64`. Тело HTTP ограничено 32 KiB, метаданные — 16 полями/2048 кодированными байтами, страницы — 100 строками. Activity возвращает `X-Read-Model: eventually-consistent`; пустая история не доказывает отсутствие счета. Ее накопительные суммы и счетчики — строки PostgreSQL `NUMERIC`, доступные средства по ним не проверяются.

По умолчанию Gateway допускает 60 аутентифицированных write-запросов за минуту на клиента совместно с другим экземпляром; лимит и окно настраиваются. Превышение дает `429 rate_limited` и `Retry-After`; недоступность Redis — `503 rate_limiter_unavailable`. Повтор с тем же ключом тоже расходует HTTP-бюджет. Окно начинается первым запросом, следующие запросы TTL не продлевают.

Успех перевода означает SQL commit, а не немедленную видимость в Activity. При Kafka outage outbox сохраняется и догоняется после восстановления. Доставка at-least-once; inbox устраняет повторный эффект. Deadline, `503` или потеря ответа оставляют исход commit неизвестным: повторите **тот же ключ с теми же параметрами** или прочитайте известный ID. Gateway повторяет только `UNAVAILABLE` один раз в общем RPC deadline 4 s; создание счета автоматически не повторяет.

Новые transfer/reversal-события начинаются с миграции `002`; исходная история не переносится автоматически. Финансирование не входит в поток. Kafka dev: один KRaft-брокер, 3 partitions, replication factor 1, minISR 1, retention 7 дней или 256 MiB на partition. Это не защита от потери узла/диска и не гарантия восстановления истекшей истории.

## Проверки и воспроизведение

Go 1.26.2; для локального race detector нужен C-компилятор. `make generate` закрепляет protoc 36.2, protoc-gen-go 1.36.12 и protoc-gen-go-grpc 1.6.2 с проверкой архива. Версии клиентов — в [go.mod](go.mod), образы tag+digest — в [compose.yaml](compose.yaml).

```sh
make generate
make test
make test-integration
make lint
make build
```

`make test-integration` запускает Linux `go test -p 1 -race -count=1 ./...` с настоящими PostgreSQL, Kafka, Redis и gRPC, выделенными тестовыми БД и уникальными topics. Без обязательных test URL обычный `go test` явно пропускает инфраструктурные сценарии. Дополнительные реальные остановки контейнеров включаются только отдельным opt-in; команды и фактические результаты приведены в [приемке расширения](docs/events/acceptance.md). Конфигурация [CI](.github/workflows/ci.yml) проверяет генерацию, формат, race, vet, Staticcheck, сборку, интеграцию и fault demo; локальная проверка не выдается за запуск hosted CI.

Новые измерения и сырые данные: [benchmarks](docs/benchmarks.md). Результаты старого HTTP/PostgreSQL пути, включая около 2248 переводов/с, сохранены отдельно в [ledger-v1](docs/benchmarks-ledger-v1.md) и к новому пути не относятся. Короткий локальный эксперимент не является SLA.

## Код и решения

| Каталог | Ответственность |
| --- | --- |
| `cmd/gateway`, `internal/gateway`, `internal/ratelimit` | HTTP, Redis admission, gRPC-клиенты |
| `cmd/ledger`, `internal/rpc`, `proto`, `gen` | Версионированные RPC, deadlines, auth, bounded concurrency |
| `internal/ledger`, `internal/store`, `migrations` | Исходные финансовые инварианты и атомарный outbox |
| `cmd/publisher`, `internal/outbox` | Lease/fencing и публикация после commit |
| `cmd/activity`, `internal/activity` | Inbox, последовательная обработка partition, SQL-проекция и gRPC-чтение |
| `internal/reconcile`, `internal/eventaudit`, `cmd/ledgerctl` | Независимые сверки, demo и измерения |
| `tests`, `tests/events` | Исходные финансовые проверки и аварийные интеграции |

[Архитектура](docs/architecture.md), [инварианты](docs/invariants.md), [RPC и ошибки](proto/README.md), [схема события](schemas/ledger-operation-v1.schema.json), [outbox](docs/events/outbox-contract.md), [Activity](docs/events/activity.md), [сбои](docs/failure-scenarios.md). Решения расширения: [события после commit](docs/events/adr-001-events-after-commit.md), [outbox/inbox](docs/events/adr-002-outbox-inbox.md), [partition и порядок](docs/events/adr-003-partition-order.md), [Redis outage](docs/events/adr-004-redis-outage.md). [10 вопросов и два пункта резюме](docs/events/interview.md).

Аудит исходной версии и базовый SHA `4a4ebd17158341f4740c31377a6d15951bc8e46f`: [baseline](docs/events/baseline.md). Расширение ведется с фактическими датами 30.10.2026 без переписывания старой истории и автоматического push. [Старая приемка](docs/acceptance.md) описывает только исходную версию; ее исторические Git-даты заданы прежним заданием.
