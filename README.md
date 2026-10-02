# Globally-Distributed Multi-Region Property Listing Backend (US & EU)

A distributed, fault-tolerant backend system simulating two geographic cloud regions (`US` and `EU`). The system demonstrates global path-based routing and automated failover via **NGINX**, asynchronous cross-region data replication using **Apache Kafka**, distributed concurrency control using **Optimistic Locking**, and duplicate-request protection via **Idempotency keys**.

---

## Architecture Overview

```mermaid
flowchart TD
    Client["Clients & Integration Test Suite"] -->|HTTP Port 8080| NGINX["NGINX Reverse Proxy (nginx_proxy)"]

    subgraph "Routing & Failover Layer"
        NGINX -->|"/us/*" (Primary Upstream)| BackendUS["backend-us (:8000)"]
        NGINX -.->|"/us/*" (Automatic Backup Failover)| BackendEU["backend-eu (:8000)"]
        NGINX -->|"/eu/*" (Primary Upstream)| BackendEU
        NGINX -.->|"/eu/*" (Automatic Backup Failover)| BackendUS
    end

    subgraph "US Region"
        BackendUS -->|"Read / Write"| DBUS[("PostgreSQL db-us (:5432)")]
    end

    subgraph "EU Region"
        BackendEU -->|"Read / Write"| DBEU[("PostgreSQL db-eu (:5433)")]
    end

    subgraph "Asynchronous Event Replication"
        BackendUS -->|"Publish: property-updates"| Kafka["Kafka Broker (:9092 / :29092)"]
        BackendEU -->|"Publish: property-updates"| Kafka
        Kafka -->|"Subscribe: origin = 'us'"| BackendEU
        Kafka -->|"Subscribe: origin = 'eu'"| BackendUS
        Kafka --- Zookeeper["Zookeeper (:2181)"]
    end
```

---

## Key Features & Core Concepts

1. **Path-Based Regional Routing & Zero-Downtime Failover (NGINX)**
   - Requests prefixed with `/us/*` route to `backend-us`.
   - Requests prefixed with `/eu/*` route to `backend-eu`.
   - If a regional backend becomes unhealthy or unreachable, NGINX instantly reroutes requests to the surviving regional backend via `proxy_next_upstream` and upstream `backup` directives.
   - NGINX access logs record `$upstream_response_time` for latency profiling.

2. **Asynchronous Cross-Region Replication (Apache Kafka)**
   - When a property is updated in a region, the service updates its local PostgreSQL database and publishes an event to the `property-updates` Kafka topic.
   - The remote region's consumer subscribes to the topic, ignores events originating from its own region, and applies foreign updates to its local database.

3. **Distributed Concurrency & Conflict Prevention (Optimistic Locking)**
   - Every property record maintains a `version` column.
   - Updates must supply the expected version number. If concurrent updates race or an update provides a stale version, the service rejects the request with **HTTP 409 Conflict**.

4. **API Idempotency (`X-Request-ID`)**
   - Clients send a unique UUID header `X-Request-ID`.
   - Processed request IDs are stored in the PostgreSQL database.
   - Any repeated request with the same `X-Request-ID` is rejected with **HTTP 422 Unprocessable Entity**.

5. **Replication Lag Monitoring**
   - Each backend provides `GET /:region/replication-lag` which computes the latency difference between the timestamp of the last consumed cross-region event and the current time.

---

## Services & Ports

| Service | Container Name | Internal Port | Host Port | Role |
| :--- | :--- | :--- | :--- | :--- |
| **NGINX** | `nginx_proxy` | `80` | `8080` | Reverse proxy, path routing, and failover |
| **Backend US** | `backend_us` | `8000` | - | FastAPI application for US region |
| **Backend EU** | `backend_eu` | `8000` | - | FastAPI application for EU region |
| **DB US** | `db_us` | `5432` | `5432` | PostgreSQL 14 instance for US region |
| **DB EU** | `db_eu` | `5432` | `5433` | PostgreSQL 14 instance for EU region |
| **Kafka** | `kafka` | `29092` | `9092` | Message broker for cross-region replication |
| **Zookeeper** | `zookeeper` | `2181` | - | Distributed coordination for Kafka |

---

## Database Schema & Seeding

Both databases (`db-us` and `db-eu`) are automatically initialized on startup using Docker's `/docker-entrypoint-initdb.d/init.sql`.

### `properties` Table Schema

```sql
CREATE TABLE IF NOT EXISTS properties (
    id BIGINT PRIMARY KEY,
    price DECIMAL(12, 2) NOT NULL,
    bedrooms INTEGER,
    bathrooms INTEGER,
    region_origin VARCHAR(2) NOT NULL,  -- 'us' or 'eu'
    version INTEGER NOT NULL DEFAULT 1, -- Optimistic locking version
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

### `idempotency_keys` Table Schema

```sql
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key VARCHAR(255) PRIMARY KEY,
    status_code INTEGER NOT NULL,
    response_body TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

The database is pre-seeded with **1,200 property records**:
- IDs `1` to `600`: `region_origin = 'us'`
- IDs `601` to `1200`: `region_origin = 'eu'`

To regenerate or customize the seeds, run:
```bash
python seeds/generate_seeds.py
```

---

## API Specification

All endpoints are accessible via the NGINX gateway at `http://localhost:8080`.

### 1. Health Check
- **Endpoint**: `GET /:region/health` (e.g., `/us/health` or `/eu/health`)
- **Status**: `200 OK`
- **Response**:
```json
{
  "status": "ok",
  "region": "us",
  "timestamp": "2026-10-02T13:45:00.123Z"
}
```

### 2. Get Property Details
- **Endpoint**: `GET /:region/properties/:id` (e.g., `/us/properties/1`)
- **Status**: `200 OK`
- **Response**:
```json
{
  "id": 1,
  "price": 322196.00,
  "bedrooms": 1,
  "bathrooms": 1,
  "region_origin": "us",
  "version": 1,
  "updated_at": "2026-10-02T13:43:19.000Z"
}
```

### 3. Update Property (Optimistic Locking & Idempotency)
- **Endpoint**: `PUT /:region/properties/:id` (e.g., `/us/properties/1`)
- **Headers**:
  - `Content-Type: application/json`
  - `X-Request-ID: <uuid>` (Optional, enforced for idempotency)
- **Request Body**:
```json
{
  "price": 500000.00,
  "version": 1
}
```
- **Response (200 OK)**:
```json
{
  "id": 1,
  "price": 500000.00,
  "version": 2,
  "updated_at": "2026-10-02T13:46:12.456Z"
}
```
- **Response (409 Conflict - Version Mismatch)**:
```json
{
  "error": "Conflict",
  "message": "Version mismatch. Current version is 2, but provided version was 1.",
  "current_version": 2,
  "provided_version": 1
}
```
- **Response (422 Unprocessable Entity - Duplicate Request)**:
```json
{
  "error": "Duplicate request",
  "message": "A request with this X-Request-ID has already been processed.",
  "request_id": "b3c9f280-77a8-4e55-8db6-4444a7bca191"
}
```

### 4. Kafka Event Published on Update
- **Topic**: `property-updates`
- **Payload**:
```json
{
  "id": 1,
  "price": 500000.00,
  "bedrooms": 1,
  "bathrooms": 1,
  "region_origin": "us",
  "version": 2,
  "updated_at": "2026-10-02T13:46:12.456Z"
}
```

### 5. Replication Lag
- **Endpoint**: `GET /:region/replication-lag` (e.g., `/eu/replication-lag`)
- **Status**: `200 OK`
- **Response**:
```json
{
  "lag_seconds": 2.5
}
```

---

## Quick Start & Verification

### Prerequisites
- Docker & Docker Compose
- Python 3.10+ (for local test runner)

### 1. Launch All Services
```bash
docker compose up -d --build
```
Wait approximately 30-60 seconds for PostgreSQL initialization, Kafka broker election, and backend health checks to pass.

Verify that all 7 containers are healthy:
```bash
docker compose ps
```

### 2. Run Automated Integration Tests
The integration test suite validates concurrent updates, race condition prevention via optimistic locking, idempotency with `X-Request-ID`, and Kafka cross-region replication:
```bash
pip install -r app/requirements.txt
pytest tests/test_optimistic_locking.py -v -s
```

### 3. Run Automated NGINX Failover Demonstration
Demonstrates zero-downtime routing when `backend_us` is stopped:
```bash
# On Linux / macOS / Git Bash:
bash tests/demonstrate_failover.sh

# Or natively on Windows PowerShell:
.\tests\demonstrate_failover.ps1
```

### 4. Inspect Upstream Response Time in NGINX Logs
Execute any request and observe the custom log format containing `$upstream_response_time`:
```bash
curl -I http://localhost:8080/us/health
docker logs nginx_proxy --tail 5
```
Example output:
```text
172.18.0.1 - - [02/Oct/2026:14:15:30 +0000] "GET /us/health HTTP/1.1" 200 68 "-" "curl/8.4.0" "-" upstream_response_time=0.003
```

---

## Conflict Resolution Strategy

When concurrent updates occur across regions for the same property, the system employs **Optimistic Concurrency Control (OCC)**:

1. **Detection**: Each record has an incrementing `version` integer. Every `UPDATE` query evaluates `WHERE id = :id AND version = :expected_version`.
2. **Rejection**: If another transaction updated the record first, the affected row count is `0`. The API returns an **HTTP 409 Conflict** status without applying the change.
3. **Client Resolution Workflow**:
   - The client catches HTTP 409.
   - The client executes a `GET /:region/properties/:id` request to fetch the latest state and current version.
   - The client presents the updated information to the user or merges business logic.
   - The client submits a new `PUT` request with the incremented version.

---

## Project Structure

```text
├── docker-compose.yml              # Multi-container orchestration (7 services)
├── Dockerfile                      # Root Dockerfile for backend services
├── .env.example                    # Environment variable template
├── README.md                       # Comprehensive documentation
├── app/
│   ├── __init__.py
│   ├── config.py                   # Pydantic Settings configuration
│   ├── database.py                 # Async PostgreSQL pool and queries
│   ├── kafka_client.py             # AIOKafka producer, consumer loop, lag tracker
│   ├── main.py                     # FastAPI application and route handlers
│   ├── requirements.txt            # Python dependencies
│   └── schemas.py                  # Pydantic models and serializers
├── backend-us/
│   └── Dockerfile                  # US service container definition
├── backend-eu/
│   └── Dockerfile                  # EU service container definition
├── nginx/
│   └── nginx.conf                  # Routing, backup upstreams, custom logs
├── seeds/
│   ├── generate_seeds.py           # Seed generator script
│   ├── init.sql                    # 1,200 property SQL seed statements
│   └── properties.csv              # CSV dataset representation
└── tests/
    ├── demonstrate_failover.sh     # Bash failover verification script
    ├── demonstrate_failover.ps1    # PowerShell failover verification script
    └── test_optimistic_locking.py  # Concurrency, idempotency & replication test suite
```
