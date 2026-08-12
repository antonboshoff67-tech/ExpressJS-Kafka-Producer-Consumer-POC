# Item Kafka Producer/Consumer POC (Express.js)

An **Express.js / Node.js** replica of the Spring Boot `item-kafka-producer-poc`
(`ConfluentCloud_Kafka-Producer-Consumer-POC`) project: a REST API that produces
and consumes `Item` records via **Apache Kafka**, replicates them from a source
database to a sink **MySQL** database through Node.js streaming pipeline jobs
(the Node equivalent of the original **Apache Flink** jobs), and demonstrates a
JWT-signed downstream gateway call. Optional **MS SQL Server** support is
included for the source database, matching the Java POC's topology.

This project intentionally mirrors the Java POC's REST paths, request/response
shapes, environment variable names, and Docker/Kubernetes deployment layout, so
the two projects can be compared side-by-side. See `ARCHITECTURE.md` for the
full rationale, including **why Node.js re-implements the Flink jobs directly**
instead of embedding a JVM-based Flink runtime.

---

## ✨ Features

- REST API for producing/consuming `Item` records to/from a shared Kafka topic (`Item_Topic` by default), built with **KafkaJS**.
- Paginated `GET /item-kafka/app/items/v1` + `GET /item-kafka/app/items/count/v1` backed by **MySQL** (default) or **MS SQL Server**.
- On-demand, time-boxed manual Kafka consumption (`POST /item-kafka/consumer/manual-consume/v1`), mirroring the Java POC's ~30s poll loop.
- Three streaming/batch **pipeline jobs** exposed under `/flink/*` (kept under the same route prefix as the Java POC for parity, even though they run natively in Node rather than inside a JVM Flink runtime):
  1. **Job 1** - source DB (MySQL or MS SQL Server) → Kafka (bounded batch of up to 100 rows).
  2. **Job 2** - Kafka → MySQL sink table (unbounded stream, batched upserts with retry, directly equivalent to Flink's `JdbcSink` batching options).
  3. **Simple Job** - dependency-free smoke test (in-memory word count) to validate the streaming-job runtime independent of Kafka/JDBC.
- Job-status polling (`GET /flink/job-status?jobName=...`) so a front end can track async job progress, exactly like the Java POC.
- JWT-signed gateway message routing demo (`POST /item-kafka/app/send-items/v1`, `GET /item-kafka/app/consume-items/v1`).
- Swagger / OpenAPI UI at `/agent/swagger-ui.html` (same path as the Java POC).
- CORS configured for a React front end (`ITEM_CORS_ALLOWED_ORIGINS`).
- 100% environment-variable-driven configuration - no hardcoded secrets or connection strings (see `.env.example`).
- Docker Compose (Kafka-only and full stack) + Dockerfile + Kubernetes/EKS manifests, mirroring the Java POC's deployment layout.

---

## 🧱 Tech Stack

| Layer | Technology |
|---|---|
| Backend framework | Express.js (Node.js) |
| Kafka client | [`kafkajs`](https://kafka.js.org/) |
| MySQL client | [`mysql2`](https://github.com/sidorares/node-mysql2) (promise API) |
| MS SQL Server client | [`mssql`](https://github.com/tediousjs/node-mssql) (optional source DB) |
| JWT | `jsonwebtoken` |
| HTTP client (gateway calls) | `axios` |
| API docs | `swagger-jsdoc` + `swagger-ui-express` |
| Logging | `winston` |
| Streaming pipeline jobs | Plain Node.js + KafkaJS (Node-native equivalent of the Java POC's Apache Flink jobs - see `ARCHITECTURE.md`) |

---

## 📁 Project Structure

```
ExpressJS-Kafka-Producer-Consumer-POC/
├── server.js                     # entry point
├── package.json
├── .env.example
├── docker-compose.full.yml       # MySQL + Kafka + backend
├── docker-compose.kafka.yml      # Kafka broker only
├── Dockerfile
├── k8s/                          # Kubernetes/EKS manifests
├── sql-scripts/                  # source/sink DDL + seed data (shared with the Java POC's schema)
└── src/
    ├── app.js                    # Express app wiring (routes, CORS, swagger, error handling)
    ├── config/                   # env.js, corsConfig.js, swaggerConfig.js
    ├── db/                       # mysqlSourcePool.js, mysqlSinkPool.js, mssqlSourcePool.js
    ├── models/                   # itemColumns.js (shared column metadata), Item.js
    ├── repositories/             # itemRepository.js
    ├── kafka/
    │   ├── kafkaClient.js
    │   ├── producer/itemProducerService.js
    │   └── consumer/itemConsumerService.js
    ├── flink/                    # Node.js equivalent of the Java POC's Flink jobs
    │   ├── jobStatus.js
    │   ├── flinkJobService.js
    │   └── jobs/
    │       ├── mssqlItemToKafkaJob.js
    │       ├── kafkaItemToMysqlJob.js
    │       └── flinkWordStreamDemoJob.js
    ├── services/msgRoutingService.js
    ├── utils/                    # jwtTokenUtil.js, logger.js
    └── routes/                   # itemRoutes.js, itemProducerRoutes.js, itemConsumerRoutes.js, msgConsumerRoutes.js, flinkJobRoutes.js
```

See **[DEVELOPER_GUIDE.md](./DEVELOPER_GUIDE.md)** for a deep, file-by-file walkthrough and **[ARCHITECTURE.md](./ARCHITECTURE.md)** for diagrams and design rationale.

---

## ✅ Prerequisites

- **Node.js 18+** (Node 20 recommended) - [https://nodejs.org](https://nodejs.org)
- **npm** (bundled with Node.js)
- A running **Kafka** broker (local, Docker, or Confluent Cloud)
- A running **MySQL 8+** instance (source + sink databases)
- Optional: **MS SQL Server** if you want to use it as the source database instead of MySQL

---

## 🚀 Setup & Running

### Quick reference - all commands (run from the repo root)

| Command | What it does |
|---|---|
| `npm install` | Installs all dependencies |
| `npm run dev` | Runs the API with **nodemon** (auto-restart on file changes) at http://localhost:8082 |
| `npm start` | Runs the API with plain `node` (production mode) |
| `docker compose -f docker-compose.kafka.yml up -d` | Starts just a local Kafka broker (KRaft mode, no Zookeeper) |
| `docker compose -f docker-compose.full.yml up -d --build` | Starts MySQL + Kafka + the backend together |

### 1. Clone / open the project

```powershell
cd C:\Workspaces\ExpressJS-Kafka-Producer-Consumer-POC
```

### 2. Configure environment variables

```powershell
Copy-Item .env.example .env
notepad .env   # fill in your local MySQL/Kafka connection details
```

### 3. Install dependencies

```powershell
npm install
```

### 4. Start Kafka + MySQL (Docker, easiest path)

```powershell
docker compose -f docker-compose.kafka.yml up -d
```

Then create the MySQL databases/tables (see `DATABASE_SETUP.md`):

```powershell
mysql -u root -p < sql-scripts\02_mysql_item_source_seed_200.sql
mysql -u root -p < sql-scripts\03_mysql_item_sink_and_consumed_tables.sql
```

### 5. Run the backend

```powershell
npm run dev
```

Backend default URL: **http://localhost:8082**
Swagger UI: **http://localhost:8082/agent/swagger-ui.html**
Health check: **http://localhost:8082/actuator/health**

### 6. Manual test commands

```powershell
curl -X GET "http://localhost:8082/item-kafka/app/items/v1?page=0&size=15"
curl -X GET "http://localhost:8082/item-kafka/app/items/count/v1"
curl -X POST "http://localhost:8082/item-kafka/app/publish-items/v1"
curl -X GET "http://localhost:8082/item-kafka/consumer/consume-status/v1"
curl -X POST "http://localhost:8082/item-kafka/consumer/manual-consume/v1" -H "Content-Type: application/json" -d "{\"groupId\":\"item_group\"}"

curl -X POST "http://localhost:8082/flink/start-job1"
curl -X POST "http://localhost:8082/flink/start-job2"
curl -X POST "http://localhost:8082/flink/stop-job2"
curl -X POST "http://localhost:8082/flink/start-simple-job"
curl -X GET  "http://localhost:8082/flink/job-status?jobName=Flink%20Job%201"
```

See `API_DOCUMENTATION.md` for full request/response examples for every endpoint.

---

## 🐳 Running with Docker Compose

```powershell
# Full stack: MySQL + Kafka + backend
docker compose -f docker-compose.full.yml up -d --build

# View logs
docker compose -f docker-compose.full.yml logs -f

# Stop everything
docker compose -f docker-compose.full.yml down
```

See `KAFKA_SETUP.md` for manual topic creation/validation commands.

---

## ☁️ AWS EKS Deployment

See:

- `AWS_README_START_HERE.md` - orientation and prerequisites
- `AWS_QUICKSTART_CHEATSHEET.md` - copy/paste command cheatsheet
- `EKS_README.md` - full EKS deployment walkthrough
- `k8s/` - the actual Kubernetes manifests (namespace, ConfigMap, Secret template, Deployment, Service, HPA, ALB Ingress)

Basic flow: create EKS cluster → create ECR repo → build & push the Docker image → apply `k8s/` manifests → point DNS at the ALB Ingress.

---

## 🔧 Configuration

All configuration is environment-variable-driven (see `.env.example` for the full list and defaults) - no credential or connection string is ever hardcoded. Key variables:

| Variable | Purpose |
|---|---|
| `ITEM_KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address(es) |
| `ITEM_KAFKA_TOPIC` | Shared topic name (default `Item_Topic`) |
| `ITEM_SOURCE_DB_TYPE` | `mysql` (default) or `mssql` |
| `ITEM_MYSQL_SOURCE_*` / `ITEM_MSSQL_*` | Source database connection settings |
| `ITEM_MYSQL_*` | Sink database connection settings |
| `ITEM_CORS_ALLOWED_ORIGINS` | Comma-separated origins allowed to call this API from a browser |
| `ITEM_JWT_PRIVATE_KEY` / `ITEM_JWT_ISSUER` / `ITEM_JWT_EXPIRY_MINUTES` | JWT signing for the gateway demo |
| `ITEM_GATEWAY_URL` | Downstream gateway endpoint for the JWT demo |
| `ITEM_JOB_BATCH_SIZE` / `ITEM_JOB_BATCH_INTERVAL_MS` / `ITEM_JOB_MAX_RETRIES` | Job 2's batch-upsert tuning (Node equivalent of Flink's `JdbcExecutionOptions`) |

---

## 📜 Related docs

| Doc | Purpose |
|---|---|
| `ARCHITECTURE.md` | Diagrams + design rationale, incl. why Flink is re-implemented natively in Node |
| `DEVELOPER_GUIDE.md` | File-by-file code walkthrough |
| `API_DOCUMENTATION.md` | Full request/response examples for every endpoint |
| `DATABASE_SETUP.md` | Source/sink schema, seed scripts, column reference |
| `SETUP_GUIDE.md` | Detailed local setup, incl. SQL Server option |
| `KAFKA_SETUP.md` | Manual Kafka topic creation/validation |
| `EKS_README.md` | Full AWS EKS deployment walkthrough |
| `AWS_README_START_HERE.md` | AWS orientation |
| `AWS_QUICKSTART_CHEATSHEET.md` | Copy/paste AWS command cheatsheet |

---

## 📜 License / Cost

This project uses only free, open-source (MIT/Apache-licensed) npm packages. There are no paid APIs or bundled paid assets - AWS/Confluent Cloud usage costs (if you choose to deploy there) are your own responsibility and are documented in the AWS docs above.

