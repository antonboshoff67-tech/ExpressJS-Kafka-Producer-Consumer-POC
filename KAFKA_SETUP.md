# Kafka Setup

## Local Kafka via Docker Compose (recommended)

```powershell
docker compose -f docker-compose.kafka.yml up -d
```

This runs a single-node Kafka broker in **KRaft mode** (no Zookeeper) on
`localhost:9092`, plus [Kafka UI](https://github.com/provectus/kafka-ui) at
`http://localhost:8080` for browsing topics/messages/consumer groups.

## Manually creating the shared topic

The app and jobs will auto-create the topic on first publish if your broker
has `auto.create.topics.enable=true` (the default for the Compose image used
here). To create it explicitly instead:

```powershell
docker exec -it item-poc-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic Item_Topic --partitions 3 --replication-factor 1
```

## Listing topics

```powershell
docker exec -it item-poc-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## Reading messages from the console (for manual validation)

```powershell
docker exec -it item-poc-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic Item_Topic --from-beginning --max-messages 5
```

## Inspecting consumer groups

```powershell
docker exec -it item-poc-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
docker exec -it item-poc-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group item_group
```

## Confluent Cloud

Set the following in `.env` and this project connects the same way:

```
ITEM_KAFKA_BOOTSTRAP_SERVERS=<your-cluster>.confluent.cloud:9092
```

If your cluster requires SASL_SSL authentication, extend
`src/kafka/kafkaClient.js`:

```js
const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: config.kafka.bootstrapServers,
  ssl: true,
  sasl: {
    mechanism: 'plain', // or 'scram-sha-256' / 'scram-sha-512'
    username: process.env.CONFLUENT_API_KEY,
    password: process.env.CONFLUENT_API_SECRET,
  },
});
```

## Stopping/cleaning up

```powershell
docker compose -f docker-compose.kafka.yml down
docker compose -f docker-compose.kafka.yml down -v   # also removes the topic data volume
```

