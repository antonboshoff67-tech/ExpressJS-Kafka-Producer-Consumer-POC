# Setup Guide

Detailed local development setup for `ExpressJS-Kafka-Producer-Consumer-POC`.

## 1. Prerequisites

- Node.js 18+ (Node 20 recommended)
- npm
- MySQL 8+ (source + sink)
- A Kafka broker (local via Docker, or Confluent Cloud)
- Optional: MS SQL Server, if you want it as the source DB

## 2. Clone and install

```powershell
cd C:\Workspaces\ExpressJS-Kafka-Producer-Consumer-POC
npm install
```

## 3. Environment variables

```powershell
Copy-Item .env.example .env
```

Edit `.env` and fill in real values for your environment. Every variable has a
safe local default already in `.env.example` (see the table in `README.md`).
**Never commit your real `.env` file** - it is already covered by `.gitignore`.

## 4. Start Kafka (choose one)

### Option A - Docker Compose (recommended)

```powershell
docker compose -f docker-compose.kafka.yml up -d
```

This starts a single-node Kafka broker in KRaft mode (no Zookeeper needed) on
`localhost:9092`, plus a Kafka UI at `http://localhost:8080` for inspecting
topics/messages.

### Option B - Confluent Cloud

Set `ITEM_KAFKA_BOOTSTRAP_SERVERS` to your Confluent Cloud bootstrap endpoint.
If your cluster requires SASL_SSL, extend `src/kafka/kafkaClient.js` with the
`ssl`/`sasl` options documented at https://kafka.js.org/docs/configuration.

## 5. Start MySQL and seed the schema

```powershell
# If you don't already have a local MySQL server:
docker run -d --name item-poc-mysql -e MYSQL_ROOT_PASSWORD=change-me -p 3306:3306 mysql:8.0

# Then seed both databases:
mysql -u root -p < sql-scripts\02_mysql_item_source_seed_200.sql
mysql -u root -p < sql-scripts\03_mysql_item_sink_and_consumed_tables.sql
```

See `DATABASE_SETUP.md` for the full schema reference and the optional SQL
Server path.

## 6. Run the app

```powershell
npm run dev
```

- API: http://localhost:8082
- Swagger UI: http://localhost:8082/agent/swagger-ui.html
- Health check: http://localhost:8082/actuator/health

## 7. Exercise the pipeline end-to-end

```powershell
# 1) Publish up to 100 rows from the source table to Kafka
curl -X POST "http://localhost:8082/item-kafka/app/publish-items/v1"

# 2) Start the batch job (source DB -> Kafka)
curl -X POST "http://localhost:8082/flink/start-job1"

# 3) Start the streaming job (Kafka -> MySQL sink) - runs indefinitely
curl -X POST "http://localhost:8082/flink/start-job2"

# 4) Poll status
curl "http://localhost:8082/flink/job-status?jobName=Flink%20Job%202"

# 5) Verify rows landed in the sink table
mysql -u root -p -e "SELECT COUNT(*) FROM item_poc.ITEM;"

# 6) Stop the stream when you're done
curl -X POST "http://localhost:8082/flink/stop-job2"
```

## 8. Common issues

| Problem | Fix |
|---|---|
| `EADDRINUSE: address already in use :::8082` | Another process (often a previous `node server.js`) is still bound to the port - stop it or change `PORT` in `.env` |
| Kafka connection errors (`ECONNREFUSED`) | Make sure the broker from step 4 is running and `ITEM_KAFKA_BOOTSTRAP_SERVERS` matches its address |
| MySQL `ER_ACCESS_DENIED_ERROR` | Double-check `ITEM_MYSQL_*` / `ITEM_MYSQL_SOURCE_*` credentials in `.env` |
| `items/count/v1` returns 500 | The source database/table doesn't exist yet - run the seed script from step 5 |
| `npm install` engine warnings about `@azure/*` | Harmless - comes from the optional `mssql` driver's Azure AD auth support; safe to ignore if you're not using Azure AD auth |

