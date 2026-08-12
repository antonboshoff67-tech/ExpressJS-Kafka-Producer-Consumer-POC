# Developer Guide

File-by-file walkthrough of `ExpressJS-Kafka-Producer-Consumer-POC`, for
developers who want to understand or extend the codebase. Written to mirror
the level of detail in the Spring Boot Java POC's own developer docs.

## 1. Entry point

### `server.js`

Starts the Express app (`src/app.js`) on `config.server.port` (default
`8082`), wires up graceful shutdown on `SIGINT`/`SIGTERM`.

### `src/app.js`

Wires together: CORS middleware, JSON body parsing, request logging, Swagger
UI at `/agent/swagger-ui.html`, a health check at `/actuator/health`, all the
route modules (mounted at the exact same path prefixes as the Java POC's
`@RequestMapping` values), a 404 fallback, and a central error handler.

## 2. Configuration (`src/config/`)

- **`env.js`** - the single source of truth for all environment-variable-driven
  settings (server port, CORS origins, Kafka, source DB, sink DB, JWT,
  gateway URL, job batching tuning, logging level). Every other module reads
  from this object instead of `process.env` directly.
- **`corsConfig.js`** - builds the `cors` middleware from `config.cors.allowedOrigins`.
- **`swaggerConfig.js`** - builds the OpenAPI spec from JSDoc `@openapi` comments in `src/routes/*.js`.

## 3. Domain model (`src/models/`)

### `itemColumns.js`

The single most important file for understanding this codebase's design: a
flat array of `{ db, js, type, pk, default }` objects describing every one of
the ~85 columns on the `Item` table. Every other layer (model mapping,
repository queries, both pipeline jobs' SQL generation and default-filling)
derives its behaviour from this one array, instead of the Java POC's approach
of hand-writing 80+ getters/setters and prepared-statement parameter bindings
per class. If you need to add/remove a column, this is the **only** file you
need to touch.

### `Item.js`

A plain class with `fromDbRow()` (build from a raw MySQL/MSSQL row),
`fromJson()` (build from a parsed Kafka message), `applyDefaults()` (fills
nulls with the `default` from `itemColumns.js`, mirroring
`ItemFromJsonFunction.applyDefaults()` in the Java POC), and `toDbValues()`
(returns an array of values in column order for a parameterised
`INSERT ... ON DUPLICATE KEY UPDATE`).

## 4. Data access (`src/db/`, `src/repositories/`)

- **`mysqlSourcePool.js`** / **`mysqlSinkPool.js`** - `mysql2/promise` connection pools for the source and sink MySQL databases respectively.
- **`mssqlSourcePool.js`** - lazily-created `mssql` connection pool, used only when `ITEM_SOURCE_DB_TYPE=mssql`.
- **`repositories/itemRepository.js`** - abstracts over both source DB types behind three methods: `findFirst100ByItemIdIsNotNull()`, `findAllPaged(page, size)`, `count()`. This is what `itemRoutes.js` and `itemProducerRoutes.js` call - neither route module needs to know which underlying DB engine is configured.

## 5. Kafka (`src/kafka/`)

- **`kafkaClient.js`** - the shared `kafkajs` `Kafka` instance, built from `config.kafka.bootstrapServers`/`clientId`.
- **`producer/itemProducerService.js`** - lazily connects a single shared producer, publishes each `Item` as JSON, splitting the batch roughly in half between `item_group`/`manual-item-group` message-key prefixes (mirrors the Java POC's `ItemProducerService.sendItems()`).
- **`consumer/itemConsumerService.js`** - `manualConsume(groupId)` opens a fresh, uniquely-named consumer group, subscribes from the beginning, and resolves after either 30 seconds elapse or 5 seconds pass with no new messages (whichever comes first) - a close behavioural match for the Java POC's time-boxed manual poll loop, adapted to KafkaJS's callback-based `consumer.run()` API instead of a blocking `poll()` loop.

## 6. Streaming/batch pipeline jobs (`src/flink/`)

See `ARCHITECTURE.md` section 2 for the full rationale on why these are
native Node.js implementations rather than embedded JVM Flink jobs.

- **`jobStatus.js`** - the four status string constants (`PENDING`/`RUNNING`/`COMPLETED`/`FAILED`).
- **`flinkJobService.js`** - orchestrates all three jobs and tracks their last-known status in an in-memory `Map`, keyed by job display name (`"Flink Job 1"`, `"Flink Job 2"`, `"Flink Simple Job"`) - deliberately kept as the same display names as the Java POC so a shared front end doesn't need to change its polling code.
- **`jobs/mssqlItemToKafkaJob.js`** - bounded batch: reads up to 100 rows from the configured source table (MySQL or MSSQL), publishes each as JSON to Kafka, then disconnects its producer. Fully awaited by `flinkJobService.runJob1()`, so its status naturally transitions to `COMPLETED`/`FAILED`.
- **`jobs/kafkaItemToMysqlJob.js`** - unbounded stream: subscribes to the shared topic, buffers incoming records, and flushes them as a single batched `INSERT ... ON DUPLICATE KEY UPDATE` statement either when the buffer reaches `ITEM_JOB_BATCH_SIZE` or on every `ITEM_JOB_BATCH_INTERVAL_MS` tick (whichever comes first) - the direct Node equivalent of Flink's `JdbcExecutionOptions`. Failed batches are retried up to `ITEM_JOB_MAX_RETRIES` times with a small linear backoff before being dropped (logged) so the stream itself never crashes. Exposes a `stop()` method (not present in the Java POC, added for graceful shutdown) that's wired up to `POST /flink/stop-job2`.
- **`jobs/flinkWordStreamDemoJob.js`** - a synchronous, dependency-free in-memory word-count "job" used purely to prove the job-runner plumbing (routes → service → status tracking) works, independent of any external system being up.

## 7. Services & utils

- **`services/msgRoutingService.js`** - `processSentMsgRequest()` builds a JWT via `jwtTokenUtil` and forwards the payload to `config.gateway.url` with `axios` (swallowing connection errors, since the downstream gateway is frequently not running in a demo environment); `processReceivedMsgRequest()` just logs.
- **`utils/jwtTokenUtil.js`** - signs a JWT with `jsonwebtoken`. If `ITEM_JWT_PRIVATE_KEY` looks like a PEM key (`BEGIN` marker present), it signs with `RS256`; otherwise it falls back to `HS256` with that same env value (or a dev-only default) as the shared secret, so the demo endpoint works even with zero JWT configuration.
- **`utils/logger.js`** - a `winston` logger writing to both the console and `logs/item-kafka-producer-poc.log`.

## 8. Routes (`src/routes/`)

Each route module corresponds 1:1 to a Java POC controller and is mounted in
`src/app.js` at the identical path prefix:

| Route module | Mounted at | Mirrors |
|---|---|---|
| `itemRoutes.js` | `/item-kafka/app` | `ItemController.java` |
| `itemProducerRoutes.js` | `/item-kafka/app` | `ItemProducerController.java` |
| `itemConsumerRoutes.js` | `/item-kafka/consumer` | `ItemConsumerController.java` |
| `msgConsumerRoutes.js` | `/item-kafka/app` | `MsgConsumerController.java` |
| `flinkJobRoutes.js` | `/flink` | `FlinkJobController.java` |

Each route handler is intentionally thin - it validates input, delegates to a
service/repository, and shapes the HTTP response - matching the "controller
does no business logic" style of the Java POC's controllers.

## 9. Extending the project

- **Real Flink integration**: see `ARCHITECTURE.md` section 2 for how to swap `flinkJobService.js`'s job bodies for calls to an external Flink cluster's REST API.
- **Server-Sent Events / streaming endpoints**: KafkaJS's `consumer.run()` callback model maps naturally onto an Express SSE endpoint (`res.write()` per message) if you want a `Flux`-like streaming GET endpoint similar to the WebFlux sibling project.
- **Authentication**: `utils/jwtTokenUtil.js` and `services/msgRoutingService.js` are ready-made building blocks for adding an `Authorization`-header-checking middleware if you want to secure the other routes too.
- **Testing**: no automated tests exist yet - `supertest` + `jest` (or `vitest`) would be a natural fit given the existing Express route structure.

