# LedgerCore: вопросы и фактические пункты резюме

Ответы относятся к существующему коду проекта; фактические прогоны отделены в [приемку](acceptance.md). Это учебное расширение одного LedgerCore с тестовыми RUB-средствами, без коммерческой эксплуатации и универсального exactly-once.

## 1. Зачем Kafka, если PostgreSQL уже фиксирует перевод?

PostgreSQL остается источником истины о средствах. Kafka асинхронно доставляет уже зафиксированные события в независимую Activity read model. Перевод не ждет готовности проекции, а broker outage превращается в durable outbox backlog. Вызов Kafka не выполняется под блокировками счетов.

## 2. Как закрыто окно между SQL commit и публикацией?

В одной Ledger-транзакции находятся журнал, две проводки, оба остатка, исход идемпотентности и outbox. Deferred constraint требует событие новой transfer/reversal-операции. Publisher сначала коротко фиксирует lease, затем публикует вне SQL и отмечает published после ack. Refusal/rollback/replay не создают новую операцию/событие. События начинаются с миграции 002; funding и прежняя история не backfill-ятся.

## 3. Почему idempotent producer не означает exactly-once?

franz-go producer с `acks=all` и включенной по умолчанию протокольной идемпотентностью защищает повторы доставки в рамках producer protocol. Если Kafka приняла запись, а процесс умер до SQL published mark, повторная outbox-публикация с тем же ID все равно возможна. Activity использует уникальный inbox и одну SQL-транзакцию для дедупликации эффекта. При RF=1 ack не обеспечивает сохранность при потере диска брокера.

## 4. Когда consumer подтверждает offset и что при rebalance?

Только после commit inbox/entries/summary или подтвержденной DLQ-публикации. Внутри каждой partition обработка последовательна, разные partitions работают с bounded concurrency. BlockRebalanceOnPoll удерживает назначение до обработки и commit успешных prefixes. Crash после SQL до offset приводит к безопасному replay; persistent ошибка останавливает consumer без пропуска записи. Затем нужен restart после восстановления зависимости.

## 5. Почему partition key — journal ID, а не account ID?

У перевода два счета, и один account key не задает порядок обоим. В v1 ключ равен journal transaction UUID, а порядок гарантируется только внутри partition. Activity использует независимые entries и коммутативные totals/counts; reversal может прийти раньше оригинала. Ее account sequence — порядок применения в проекции, не глобальное время commit. Денежное решение остается в Ledger.

## 6. Что происходит с плохим событием и недоступной DLQ?

Malformed, несовместимая схема/type и конфликт содержимого одного ID — permanent ошибки. Consumer публикует DLQ envelope со стабильным source `topic/partition/offset`, classification, bounded payload и hash. Только ack DLQ разрешает source offset commit. Ошибка DLQ не считается обработкой. Повторы DLQ допустимы и имеют ту же source identity; transient DB errors ограниченно повторяются отдельно.

## 7. Как безопасно восстановить результат после gRPC deadline?

Deadline не доказывает rollback: SQL мог успеть commit до потери ответа. Клиент повторяет тот же idempotency key и параметры либо читает известный operation ID. Gateway автоматически делает максимум один retry только UNAVAILABLE в общем deadline 4 s с неизменными данными/key. Unkeyed CreateAccount не повторяется. Built-in gRPC retries отключены; SQL ошибки наружу не выдаются.

## 8. Зачем Lua в Redis и какой результат при outage?

Один script атомарно делает INCR, PEXPIRE только при первом запросе и PTTL. Два Gateway используют SHA-256 identity одного аутентифицированного клиента и общий write key. Последующие запросы не продлевают окно. Превышение — 429 и rounded-up Retry-After; недоступность — fail-closed 503 до вызова Ledger. Идемпотентный HTTP-повтор тоже расходует бюджет. Потеря Redis сбрасывает окно; ограничитель не является финансовой гарантией.

## 9. Как сохранены исходные финансовые инварианты?

HTTP compatibility и gRPC используют один Store. В READ COMMITTED writer резервирует key/fingerprint и блокирует счета по возрастанию UUID, проверяет остаток/int64 overflow и атомарно фиксирует двойную запись. Отмена — новая полная обратная операция. Ограничения, immutable journal grants/triggers и уникальность reversal дополняют приложение. Отдельная сверка использует NUMERIC и согласованный REPEATABLE READ READ ONLY snapshot. PostgreSQL owner не входит в эту защитную границу.

## 10. Что доказывает replay, и как не перепутать статистику с балансом?

Новый consumer group повторно читает retained range в ту же БД; уникальный inbox делает повтор no-op, ID sets, entries и NUMERIC totals/counts остаются равными. Сверка отдельно реконструирует ожидаемые данные из journal/outbox и проверяет проекцию при quiescent writers. Inbox не очищается автоматически; Kafka retention ограничивает доступную историю. Activity sums/counts отражают операции, включая отдельные отмены, и не являются доступными средствами. Пустая Activity не доказывает отсутствие счета. Точные measurements и ограничения приводятся отдельно, старые 2248 tx/s на новый путь не переносятся.

## Два пункта для существующего блока LedgerCore в резюме

- Расширил Go/PostgreSQL LedgerCore сервисами Gateway и Activity: версионированный gRPC, Kafka transactional outbox/inbox и общий Redis Lua rate limiter двух Gateway, сохранив двойную запись и финансовую идемпотентность.
- Реализовал восстановление после повторной доставки и сбоев publisher/consumer, DLQ и replay; добавил интеграционные проверки с настоящими PostgreSQL, Kafka и Redis и независимую сверку событийной проекции с журналом.

Не добавлять отдельный пятый проект, коммерческий опыт или неподтвержденный throughput/SLA. Эти пункты обновляют один существующий проект LedgerCore.
