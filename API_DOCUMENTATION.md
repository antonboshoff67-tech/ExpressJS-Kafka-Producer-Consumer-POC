# API Documentation

Base URL (local): `http://localhost:8082`
Swagger UI: `http://localhost:8082/agent/swagger-ui.html`

---

## Items

### `GET /item-kafka/app/items/v1`

Paginated list of Item rows from the source database.

**Query params:** `page` (default `0`), `size` (default `15`)

```powershell
curl "http://localhost:8082/item-kafka/app/items/v1?page=0&size=15"
```

**Response 200:**
```json
{
  "content": [ { "itemId": "ITM0001", "itemLevel": 1, "...": "..." } ],
  "totalElements": 200,
  "totalPages": 14,
  "number": 0,
  "size": 15,
  "first": true,
  "last": false
}
```

### `GET /item-kafka/app/items/count/v1`

```powershell
curl "http://localhost:8082/item-kafka/app/items/count/v1"
```

**Response 200:** `200` (plain number)

---

## Producer

### `POST /item-kafka/app/publish-items/v1`

Reads up to 100 Item rows from the source table and publishes them to Kafka.

```powershell
curl -X POST "http://localhost:8082/item-kafka/app/publish-items/v1"
```

**Response 200 (text):** `Items sent to Kafka topic successfully!`

---

## Consumer

### `GET /item-kafka/consumer/consume-status/v1`

```powershell
curl "http://localhost:8082/item-kafka/consumer/consume-status/v1"
```

**Response 200 (JSON string):** `"Consumer is not running."`

### `POST /item-kafka/consumer/manual-consume/v1`

Time-boxed (~30s) poll of the shared topic using the requested logical group.

```powershell
curl -X POST "http://localhost:8082/item-kafka/consumer/manual-consume/v1" `
  -H "Content-Type: application/json" `
  -d "{ \"groupId\": \"item_group\" }"
```

**Response 200 (text):** `Manually consumed 12 items.`

Valid `groupId` values: `item_group`, `manual-item-group`. Anything else
returns: `Incorrect consumer group. Use item_group or manual-item-group.`

---

## Gateway message routing demo (JWT-signed)

### `POST /item-kafka/app/send-items/v1`

```powershell
curl -X POST "http://localhost:8082/item-kafka/app/send-items/v1" `
  -H "Content-Type: application/json" `
  -d "{ \"message\": \"hello\" }"
```

**Response 200 (text):** `The items were prepared for publishing to the Kafka topic.`

### `GET /item-kafka/app/consume-items/v1`

```powershell
curl -X GET "http://localhost:8082/item-kafka/app/consume-items/v1" `
  -H "Content-Type: application/json" `
  -H "Authorization: Bearer <token>" `
  -d "{ \"message\": \"hello\" }"
```

**Response 200 (text):** `Message items were processed successfully from the Kafka consumer.`

---

## Streaming/batch pipeline jobs (Node.js equivalent of the Java POC's Flink jobs)

### `POST /flink/start-job1`

Source DB → Kafka batch job (up to 100 rows). Fire-and-forget; responds
immediately.

```powershell
curl -X POST "http://localhost:8082/flink/start-job1"
```

**Response 200 (text):** `Flink Job 1 started successfully.`

### `POST /flink/start-job2`

Kafka → MySQL unbounded streaming job. Fire-and-forget; keeps running until
stopped.

```powershell
curl -X POST "http://localhost:8082/flink/start-job2"
```

**Response 200 (text):** `Flink Job 2 started successfully.`

### `POST /flink/stop-job2`

Stops the currently-running Job 2 stream (not present in the Java POC, added
here since Node processes are long-lived and you may want a clean way to
release the Kafka consumer group / DB pool without restarting the server).

```powershell
curl -X POST "http://localhost:8082/flink/stop-job2"
```

### `POST /flink/start-simple-job`

Dependency-free smoke test job, awaited synchronously.

```powershell
curl -X POST "http://localhost:8082/flink/start-simple-job"
```

**Response 200 (text):** `Flink Simple Job executed successfully.`

### `GET /flink/job-status`

```powershell
curl "http://localhost:8082/flink/job-status?jobName=Flink%20Job%201"
```

**Response 200 (JSON string):** `"RUNNING"` (one of `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`)

---

## Health check

### `GET /actuator/health`

```powershell
curl "http://localhost:8082/actuator/health"
```

**Response 200:** `{"status":"UP"}`

