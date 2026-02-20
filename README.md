# shori-realtime — Phase 1: CDC Infrastructure & Connection Scaffold

A PoC that captures CDC events from SQL Server via Debezium Server, routes them through Redpanda, and consumes them in a bare-bones Rust application.

## Architecture

```
SQL Server (CDC) ──► Debezium Server ──► Redpanda ──► Rust Consumer
     :1433                                :9092           (host)
                                          :19092
```

## Prerequisites

- Docker & Docker Compose
- Rust toolchain (cargo, rustc)
- `cmake` and `libcurl4-openssl-dev` (for building rdkafka — `sudo apt install cmake libcurl4-openssl-dev`)

## Quick Start

### 1. Start the infrastructure

```bash
docker compose up -d
```

Wait for all services to become healthy:

```bash
docker compose ps
```

You should see `sqlserver` and `redpanda` marked as `healthy`. Debezium will start once both dependencies are healthy.

### 2. Initialize the database

Run the init script against SQL Server to create the database, enable CDC, and create the `Users` table:

```bash
docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStr0ngP@ssw0rd!' -C \
  -i /dev/stdin < init.sql
```

### 3. Sanity Check — Insert test data

Open a `sqlcmd` session to insert some rows:

```bash
docker exec -it sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourStr0ngP@ssw0rd!' -C -d ShoriDB
```

Then run:

```sql
INSERT INTO dbo.Users (name, status) VALUES ('Alice', 'active');
INSERT INTO dbo.Users (name, status) VALUES ('Bob', 'inactive');
GO

UPDATE dbo.Users SET status = 'active', updated_at = GETDATE() WHERE name = 'Bob';
GO
```

### 4. Verify events in Redpanda

List the topics that Debezium has created:

```bash
docker exec -it redpanda rpk topic list
```

You should see `shori_data.ShoriDB.dbo.Users` (and `schema-changes.shori_data`).

Consume messages from the CDC topic:

```bash
docker exec -it redpanda rpk topic consume shori_data.ShoriDB.dbo.Users
```

You'll see the raw JSON CDC events printed to your terminal in real-time. Press `Ctrl+C` to stop.

### 5. Run the Rust consumer

In a separate terminal:

```bash
cargo run
```

The consumer connects to Redpanda on `localhost:19092`, subscribes to `shori_data.ShoriDB.dbo.Users`, and prints every CDC event (key + payload) to stdout.

Go back to your `sqlcmd` session and insert/update more rows — you'll see them appear in the Rust consumer output in real-time.

## Project Structure

```
.
├── docker-compose.yml          # SQL Server + Redpanda + Debezium Server
├── conf/
│   └── application.properties  # Debezium Server configuration
├── init.sql                    # Database bootstrap (CDC setup)
├── src/
│   └── main.rs                 # Rust consumer entry point
├── Cargo.toml                  # Rust project manifest
├── PLAN.md                     # Phase 1 implementation plan
└── README.md                   # This file
```

## Troubleshooting

**Debezium keeps restarting / can't connect to SQL Server:**
- Ensure SQL Server is fully healthy before Debezium starts (`docker compose ps`).
- Check Debezium logs: `docker compose logs debezium`.
- Verify CDC is enabled: connect to SQL Server and run `SELECT is_cdc_enabled FROM sys.databases WHERE name = 'ShoriDB';` — should return `1`.

**Rust consumer doesn't receive messages:**
- Make sure Redpanda port `19092` is exposed and reachable from the host.
- Verify the topic exists: `docker exec -it redpanda rpk topic list`.
- Check consumer group lag: `docker exec -it redpanda rpk group describe shori-realtime-consumer`.

**SQL Server Agent not running (CDC won't capture changes):**
- The `MSSQL_AGENT_ENABLED=true` environment variable must be set in docker-compose. Verify with: `docker exec -it sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStr0ngP@ssw0rd!' -C -Q "SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE '%Agent%'"`.
