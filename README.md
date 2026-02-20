# shori-realtime — Phase 2: CDC Real-Time Dashboard PoC

A PoC that captures CDC events from multiple SQL Server instances via multiple Debezium Server instances, routes them through Redpanda, and serves them to a visually pleasing real-time web dashboard using a Python FastAPI backend and a React/Tremor frontend.

## Architecture

This PoC showcases a complete end-to-end low-latency pipeline. A workload generator "hammers" 2 distinct SQL Server databases (`ShoriDB_1` and `ShoriDB_3`) with continuous batch inserts. Debezium Server reads these events, routes them to Redpanda, and a FastAPI backend exposes them via Server-Sent Events (SSE) to a live React dashboard.

```
ShoriSQL_1 (CDC) ──► Debezium 1 ──┐
  (ShoriDB 1 & 2)                 │
     :1433                        │
                                  ▼
                              Redpanda ──► Python FastAPI Backend ──► React Dashboard
                                  ▲          (SSE via port 8000)      (Vite port 5173)
ShoriSQL_2 (CDC) ──► Debezium 2 ──┘           
  (ShoriDB 3 & 4)
     :1434

* Workload Generator continuously writes to ShoriDB_1 and ShoriDB_3 via pyodbc
```

## Prerequisites

- Docker & Docker Compose
- `uv` (Fast Python package and project manager)
- `Node.js` & `npm` (for the React Dashboard)

## Quick Start

### 1. Start & Initialize the infrastructure

Instead of running Docker Compose manually, use the provided reset script. It tears down old state, brings up the containers, waits for health checks, and initializes all 4 databases with the CDC schema.

```bash
./scripts/reset.sh
```

### 2. Run the FastAPI Backend

In a new terminal, use `uv` to start the backend. This spins up the FastAPI server on port 8000, which includes a background-threaded Redpanda Kafka consumer bridging events to an async Server-Sent Events (SSE) endpoint:

```bash
uv run main.py
```

### 3. Start the React Dashboard

In a separate terminal, install the dependencies and start the Vite dev server for the frontend. The dashboard uses Tremor UI and Tailwind CSS for a modern, dark-mode real-time visual representation:

```bash
cd frontend
npm install
npm run dev
```

Open your browser to `http://localhost:5173`. The dashboard should be live and waiting for data.

### 4. Run the Workload Generator

Finally, in a separate terminal, start the simulated workload generator. This script continuously generates random data batches and inserts them directly into `ShoriDB_1` and `ShoriDB_3` without sleeping, purposefully simulating high-throughput spikes:

```bash
uv run generator.py
```

Watch the dashboard light up! You should see live metric cards updating, raw CDC JSON payloads streaming in the terminal view, and the throughput AreaChart displaying the events/sec rate.

## Project Structure

```
.
├── docker-compose.yml          # 2x SQL Server + Redpanda + 2x Debezium Server
├── conf/
│   ├── debezium1/application.properties  # Debezium 1 configuration (reduced poll interval)
│   └── debezium2/application.properties  # Debezium 2 configuration (reduced poll interval)
├── db/
│   ├── init_sql1.sql           # Database bootstrap for Instance 1
│   └── init_sql2.sql           # Database bootstrap for Instance 2
├── frontend/                   # React + Vite + Tremor + Tailwind dashboard
│   ├── src/App.tsx             # Main dashboard UI and SSE stream processing
│   └── tailwind.config.js      # UI styling configuration
├── scripts/
│   └── reset.sh                # Helper script to clean, start, and init infrastructure
├── pyproject.toml              # Python uv project manifest
├── main.py                     # FastAPI SSE backend & async Kafka consumer
├── generator.py                # Simulated pyodbc high-throughput workload generator
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
- Check consumer group lag: `docker exec -it redpanda rpk group describe shori-python-consumer`.

**SQL Server Agent not running (CDC won't capture changes):**
- The `MSSQL_AGENT_ENABLED=true` environment variable must be set in docker-compose. Verify with: `docker exec -it shorisql_1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStr0ngP@ssw0rd!' -C -Q "SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE '%Agent%'"`
