# Аварийные сценарии LedgerCore events

Проверки используют настоящие PostgreSQL/Kafka/Redis и TCP gRPC, выделенные тестовые БД и уникальные topics. Точки принудительного сбоя доступны тестовым subprocess/интеграциям, не публичному API. Таблица описывает проверяемый контракт; фактические запуски и результаты находятся в [приемке расширения](events/acceptance.md), результаты полного demo — в [demo](demo.md). Бизнес-ошибки, инфраструктурные ошибки, 429 и DLQ учитываются раздельно.

| Сценарий | Ожидаемое поведение и проверка |
| --- | --- |
| 1. Gateway → gRPC → transfer | Один журнал, две проводки, один outbox ID, совпадающий с operation ID; повтор с тем же ключом возвращает исходный результат. `TestGatewayGRPCCommitOutcomeAndSharedRedisBudget`. |
| 2. Бизнес-отказ/rollback | Нет нового журнала/outbox. Финальный отказ сохраняется идемпотентно; откат не оставляет частичную операцию. Исходные финансовые тесты и `TestPublisherRecoveryFencingAndActivityConvergence`. |
| 3. Commit выполнен, ответ потерян/deadline | Повтор исходного ключа с теми же параметрами возвращает одну исходную операцию без двойного списания. Настоящие TCP-сценарии `TestRealTCPRetryReusesCommittedKey` и Gateway integration; серверный deadline проверяет `TestDeadlineCancelsServerWork`. |
| 4. Реальная остановка Kafka во время команд | SQL commit остается успешным, непереданный outbox накапливается. После восстановления/restart publisher и Activity догоняют до равных множеств IDs. Container-fault ветка `TestPublisherRecoveryFencingAndActivityConvergence`, полный `make demo`. |
| 5. Publisher умер после Kafka ack до SQL mark | Тестовый subprocess доставляет запись и завершается до Ack. Истекшая аренда захватывается заново, event ID сохраняется; возможный Kafka-дубликат не меняет итог Activity. Там же проверяются два publisher и отказ обновления устаревшим владельцем. |
| 6. Activity умерла после SQL commit до offset | Kafka повторяет запись; inbox сохраняет ровно один эффект. `TestKafkaActivityProcessDeathAfterSQLBeforeOffset`. |
| 7. Два consumers/rebalance | Перераспределение partitions сохраняет полноту inbox/entries, обработка partition последовательна. `TestKafkaConsumerReplayRebalanceAndDLQ`. При трех partitions полезная параллельность группы ограничена тремя. |
| 8. БД Activity временно недоступна | Три ограниченные попытки, затем ошибка/остановка без пропуска offset; после восстановления и restart запись доходит. `TestKafkaDatabaseFailureAndDLQFailureDoNotCommit`. |
| 9. Дубликат, перестановка, поздняя отмена | Одинаковый event ID — no-op; totals/counts не зависят от порядка независимых событий. Отмена до оригинала разрешена. `TestActivityAtomicReplayAndUnboundedTotals`, `TestActivityLateReversalAndStablePagination`. |
| 10. Poison event и сбой DLQ | Ошибочная/неподдерживаемая запись классифицируется; source offset продвигается только после подтвержденной DLQ-записи. Реальная отклоненная DLQ-публикация оставляет offset. `TestKafkaConsumerReplayRebalanceAndDLQ`, `TestKafkaDatabaseFailureAndDLQFailureDoNotCommit`. |
| 11. Replay новым group ID | Сохраненный диапазон повторно читается в существующую БД, множества inbox/entries и NUMERIC-сводки до/после равны. `TestKafkaConsumerReplayRebalanceAndDLQ`, CLI `events-demo --phase replay`. |
| 12. Два Gateway/конкуренция/TTL | Оба экземпляра расходуют один Redis-ключ; число допусков в одном окне не превышает общий limit. Следующие запросы не продлевают TTL; новое окно снова допускает запросы. `internal/ratelimit/redis_test.go`, Gateway integration, demo limiter. |
| 13. Реальная остановка Redis | Аутентифицированный write возвращает `503 rate_limiter_unavailable` до Ledger; это отличается от `429 rate_limited` и не создает финансовую резервацию. Container-fault ветка Gateway integration, demo `redis-outage`. |
| 14. SIGTERM компонентов | HTTP/RPC и фоновые процессы завершаются ограниченно; outbox сохраняется, после restart обе сверки проходят. Demo проверяет настоящий Docker stop и exit code каждого Go-компонента, затем восстановление. Фактические времена приводятся в отчете, не являются SLA. |
| 15. Независимые сверки | Финансовый REPEATABLE READ snapshot, отдельная проверка inbox/entries/NUMERIC и сравнение журнал/outbox/inbox ID/payload. `TestActivitySnapshotAuditUnderLoadAndCorruption`, `internal/eventaudit` и финальные demo verify/reconcile. |

Исходники: [tests/events](../tests/events/events_test.go), [RPC TCP tests](../internal/rpc/transport_test.go), [consumer tests](../internal/activity/consumer_test.go), [Activity SQL tests](../internal/activity/store_test.go), [outbox tests](../internal/outbox/outbox_test.go), [исходные финансовые проверки](../tests). Дополнительный regression `TestDeliveryResultsFollowRecordNotCompletionOrder` проверяет сопоставление асинхронных результатов публикации по записи, чтобы частичный успех разных partitions не пометил чужое событие published.

## Воспроизведение

```sh
make up
make test-integration
make demo
make reconcile
```

Suite требует интеграционные переменные и падает при отсутствии инфраструктуры в обязательном режиме. Внутри Compose они заданы автоматически. Обычный локальный `go test ./...` без URL не доказывает интеграционную приемку: инфраструктурные tests явно skip. Реальные container stop tests включаются отдельно `LEDGER_ALLOW_CONTAINER_FAULTS=1` с IDs именно тестовых Kafka/Redis; [CI](../.github/workflows/ci.yml) содержит полный пример loopback-инфраструктуры и env. Не передавайте IDs посторонних контейнеров.

`make demo` штатно останавливает компоненты **своего** Compose-проекта, создает явно учтенные тестовые операции, проверяет convergence/replay/limiter и восстанавливает сервисы. Во время broker outage Activity может завершиться после persistent fetch error; восстановление включает restart Activity, а не только запуск брокера. Raw отчеты, фазы и component logs сохраняются в `.runtime/events-demo/<run-id>/`.

## Что сравнивать

Для каждого валидного исходного события должны совпасть `journal_transaction_id`, outbox event ID, inbox ID и канонические поля payload; в Activity — две строки для разных счетов и правильные NUMERIC-сводки. Kafka deliveries могут быть больше уникальных событий. Poison/DLQ не входят в валидный финансовый набор и считаются отдельно. Ноль backlog сам по себе не доказывает полноту: независимый audit дополнительно сверяет ожидаемый journal/event набор.

Cross-DB сверку полноты запускайте при остановленной внешней записи и после догона. Финансовый аудит имеет собственный согласованный snapshot, но распределенного snapshot двух БД здесь нет. Предмиграционные операции и funding исключены из событий по контракту; сверка не создает искусственный backfill.

## Границы экспериментов

RF=1 означает отсутствие защиты от потери диска/единственного брокера; stop/start проверяет восстановление доставки, а не HA Kafka-кластера. Retention ограничивает replay, а inbox не очищается автоматически. Потеря Redis-данных сбрасывает request budget, не денежную идемпотентность. Недостаток PostgreSQL-диска может остановить новые переводы целиком; hard quota для outbox не реализована. Истекшая или отсутствующая неархивированная Kafka-история не восстанавливается обещанием «replay».
