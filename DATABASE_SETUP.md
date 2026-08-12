# Database Setup

This project reuses the exact same schema/seed scripts as the Spring Boot
Java POC (`ConfluentCloud_Kafka-Producer-Consumer-POC/sql-scripts/`), copied
verbatim into `sql-scripts/` here, so the two backends can point at the same
databases interchangeably.

## 1. Databases

| Database | Role | Used by |
|---|---|---|
| `item_poc_source` (MySQL) or SQL Server equivalent | **Source** - read from | `itemRepository.js`, `mssqlItemToKafkaJob.js` |
| `item_poc` (MySQL) | **Sink** - written to | `kafkaItemToMysqlJob.js` |

## 2. Scripts (run in this order)

| Script | Purpose |
|---|---|
| `sql-scripts/01_mssql_item_seed_200.sql` | Optional: seeds a SQL Server `ITEM` source table with 200 rows (only needed if `ITEM_SOURCE_DB_TYPE=mssql`) |
| `sql-scripts/02_mysql_item_source_seed_200.sql` | Seeds a MySQL `item_poc_source.ITEM` table with 200 rows (default source) |
| `sql-scripts/03_mysql_item_sink_and_consumed_tables.sql` | Creates the MySQL `item_poc.ITEM` sink table (left empty - populated by the pipeline) + an optional `ITEM_CONSUMED` audit table |

```powershell
mysql -u root -p < sql-scripts\02_mysql_item_source_seed_200.sql
mysql -u root -p < sql-scripts\03_mysql_item_sink_and_consumed_tables.sql
```

For SQL Server as the source instead:

```powershell
sqlcmd -S localhost -U sa -P "<password>" -i sql-scripts\01_mssql_item_seed_200.sql
```

Then set in `.env`:
```
ITEM_SOURCE_DB_TYPE=mssql
ITEM_MSSQL_HOST=localhost
ITEM_MSSQL_USERNAME=sa
ITEM_MSSQL_PASSWORD=<password>
```

## 3. Column reference

The full 80+ column list (identical to the Java POC's `Item.java`) lives in
one place in this project: **`src/models/itemColumns.js`**. Every layer
(model, repository, both pipeline jobs) derives its behaviour from that single
array instead of duplicating column names, so it is the authoritative
reference for the JSON field name ↔ DB column name mapping (e.g. `itemId` ↔
`item_id`, `wwStaticMass` ↔ `ww_static_mass`).

## 4. Recommended dedicated MySQL user

```sql
CREATE USER 'item_poc_user'@'%' IDENTIFIED BY 'change-me-strong-password';
GRANT ALL PRIVILEGES ON item_poc.* TO 'item_poc_user'@'%';
GRANT ALL PRIVILEGES ON item_poc_source.* TO 'item_poc_user'@'%';
FLUSH PRIVILEGES;
```

Then set `ITEM_MYSQL_USERNAME` / `ITEM_MYSQL_PASSWORD` /
`ITEM_MYSQL_SOURCE_USERNAME` / `ITEM_MYSQL_SOURCE_PASSWORD` accordingly in `.env`.

