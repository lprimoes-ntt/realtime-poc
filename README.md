# shori-realtime — Phase 2: CDC Real-Time Dashboard PoC

A PoC that captures CDC events from multiple SQL Server instances via multiple Debezium Server instances, routes them through Redpanda, and serves them to a visually pleasing real-time web dashboard using a Python FastAPI backend and a React/Tremor frontend.

## Architecture & Recent Changes

This PoC showcases a complete end-to-end low-latency pipeline combined with a real-time Medallion Architecture Lakehouse:
- **Workload Generator**: Parallelized via `concurrent.futures` to hammer `ShoriDB_1` and `ShoriDB_2` simultaneously with simulated ticket data.
- **CDC Pipeline**: Debezium Server reads these events, routing them to Redpanda.
- **Medallion Lakehouse**: A background thread in the FastAPI backend buffers Kafka messages into micro-batches, processing them via `polars` and `deltalake`.
  - **Bronze**: Appends raw JSON payloads.
  - **Silver**: Cleans and strictly upserts (merges) the tickets.
  - **Gold**: Aggregates ticket status counts per source database.
  *(Note: Currently, the Delta tables are written to the local `data/datalake/` directory rather than the Azurite emulator due to `deltalake` Python library compatibility with the local emulator.)*
- **Real-Time UI**: The FastAPI backend exposes the Gold layer aggregations via Server-Sent Events (SSE) to a live React dashboard, rendering a live Tremor `BarChart` of Tickets by Status per Database.

```text
ShoriSQL_1 (CDC) ──► Debezium 1 ──┐
  (ShoriDB_1)                     │
     :1433                        │
                                  ▼
                              Redpanda ──► Python FastAPI Backend ──► React Dashboard
                                  ▲        (Micro-batch Lakehouse)    (Vite port 5173)
ShoriSQL_2 (CDC) ──► Debezium 2 ──┘        (SSE via port 8000)
  (ShoriDB_2)
     :1434
```

## Prerequisites

- Docker & Docker Compose
- `uv` (Fast Python package and project manager)
- `Node.js` & `npm` (for the React Dashboard)

## Quick Start

### 1. Start & Initialize the infrastructure

Instead of running Docker Compose manually, use the provided setup script. It tears down old state, brings up the containers, waits for health checks, and initializes the databases with the CDC schema.

```bash
./scripts/setup.sh
```

### 2. Run the FastAPI Backend

In a new terminal, use `uv` to start the backend. This spins up the FastAPI server on port 8000 using the standard runner. The backend includes background threads for Kafka consumption and Lakehouse micro-batching.

```bash
uv run fastapi dev main.py
```

### 3. Start the React Dashboard

In a separate terminal, install the dependencies and start the Vite dev server for the frontend. The dashboard uses Tremor UI and Tailwind CSS for a modern, dark-mode real-time visual representation:

```bash
cd frontend
npm install --legacy-peer-deps
npm run dev
```

Open your browser to `http://localhost:5173`. The dashboard should be live and waiting for data.

### 4. Run the Workload Generator

Finally, in a separate terminal, start the simulated workload generator. This script continuously generates random data batches and inserts them concurrently into `ShoriDB_1` and `ShoriDB_2` without sleeping, purposefully simulating high-throughput spikes:

```bash
uv run generator.py
```

Watch the dashboard light up! You should see live metric cards updating and the BarChart displaying the real-time aggregated Medallion Gold metrics.

## Project Structure

```text
.
├── docker-compose.yml          # 2x SQL Server + Redpanda + 2x Debezium Server (+ Azurite)
├── conf/
│   ├── debezium1/application.properties
│   └── debezium2/application.properties
├── data/datalake/              # Local storage for Bronze, Silver, and Gold Delta tables
├── db/
│   ├── init_sql1.sql           # Database bootstrap for Instance 1
│   └── init_sql2.sql           # Database bootstrap for Instance 2
├── frontend/                   # React + Vite + Tremor + Tailwind dashboard
│   ├── src/App.tsx             # Main dashboard UI and SSE stream processing
│   └── tailwind.config.js      # UI styling configuration
├── scripts/
│   └── setup.sh                # Helper script to clean, start, and init infrastructure
├── pyproject.toml              # Python uv project manifest
├── main.py                     # FastAPI SSE backend, async Kafka consumer & Lakehouse loop
├── processor.py                # Core Polars & Delta Lake (Medallion) logic
├── generator.py                # Concurrent pyodbc high-throughput workload generator
└── README.md                   # This file
```

## Troubleshooting

**Debezium keeps restarting / can't connect to SQL Server:**
- Ensure SQL Server is fully healthy before Debezium starts (`docker compose ps`).
- Check Debezium logs: `docker compose logs debezium_1` or `docker compose logs debezium_2`.
- Verify CDC is enabled: connect to SQL Server and run `SELECT is_cdc_enabled FROM sys.databases WHERE name = 'ShoriDB_1';` — should return `1`.

**FastAPI / Python backend doesn't receive messages:**
- Make sure Redpanda port `19092` is exposed and reachable from the host.
- Verify the topic exists: `docker exec -it redpanda rpk topic list`.

**SQL Server Agent not running (CDC won't capture changes):**
- The `MSSQL_AGENT_ENABLED=true` environment variable must be set in docker-compose. Verify with: `docker exec -it shorisql_1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStr0ngP@ssw0rd!' -C -Q "SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE '%Agent%'"`
