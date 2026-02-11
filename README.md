# Solana Liquidity Sniper Dashboard (MVP)

## Project Overview

This project implements an End-to-End Data Engineering Pipeline designed to ingest, process, and visualize real-time Solana blockchain transaction logs. The goal is to build the foundation for a "Liquidity Sniper" dashboard that can identify significant on-chain events, particularly those related to liquidity changes in Decentralized Finance (DeFi) protocols on Solana.

The MVP showcases real-time data streaming, custom log decoding, robust data storage, and an API-driven dashboard.

**Key Technologies Used:**
*   **Solana WebSockets:** Real-time data ingestion from the Solana Devnet.
*   **Apache Kafka:** High-throughput, fault-tolerant message queue for buffering raw transaction logs.
*   **PostgreSQL:** Relational database for persistent storage of structured, decoded transaction data.
*   **FastAPI:** Python web framework for building a high-performance API to serve processed data.
*   **Streamlit:** Python library for rapidly building interactive web applications/dashboards for visualization.
*   **Docker & Docker Compose:** For local development and orchestration of Kafka, Zookeeper, and PostgreSQL services.
*   **Python:** Primary language for all backend services and the dashboard.

## Architecture

```
Solana Devnet WebSocket
        ↓ (Raw Logs)
[Python Producer (solana.py)]
        ↓
    Kafka Topic
        ↓
[Python Processor (processor.py)] (Consumes, Decodes, Writes)
        ↓
   PostgreSQL Database
        ↓
[FastAPI Backend (main.py)] (Serves Data via API Endpoints)
        ↓
[Streamlit Dashboard (dashboard.py)] (Consumes API, Visualizes Data)
```

## Features

### Current MVP Capabilities
*   **Real-time Solana Log Ingestion:** Connects to a public Solana Devnet WebSocket and streams `logsNotification` messages to Kafka.
*   **Robust Kafka Pipeline:** Utilizes Kafka for buffering and reliable delivery of raw transaction logs.
*   **Custom Solana Log Decoding:** Partially decodes raw Solana log messages, identifying `program_id` and a generic `event_type`.
*   **Structured Data Persistence:** Stores decoded transaction data in a PostgreSQL database.
*   **FastAPI Data API:** Exposes processed data via RESTful API endpoints, including recent transactions and aggregated analytics (counts by event type and program ID).
*   **Interactive Streamlit Dashboard:** Visualizes recent transactions and activity distributions with auto-refresh.

## How to Run Locally

Follow these steps to set up and run the entire pipeline on your local machine:

### Prerequisites
*   Docker and Docker Compose installed.
*   Python 3.8+ and `pip` (or `pipenv`, `conda`).
*   Git for cloning the repository.

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/solana-dashboard.git # Replace with your repo
cd solana-dashboard
```

### 2. Set up Python Environment & Install Dependencies

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Create a `.env` file in the root directory of the project with the following content. Replace placeholders as needed.

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_RAW_TRANSACTIONS_TOPIC=solana_raw_transactions
DATABASE_URL=postgresql+psycopg2://user:password@localhost:5432/solana_liquidity
FASTAPI_BASE_URL=http://localhost:8000
```

### 4. Start Docker Services (Kafka, Zookeeper, PostgreSQL)

```bash
docker-compose up -d
```
Verify containers are running: `docker-compose ps`

### 5. Create Kafka Topic

Explicitly create the Kafka topic, as `confluent-kafka` client doesn't auto-create by default.

```bash
docker exec -it kafka_broker kafka-topics --create --topic solana_raw_transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 6. Create PostgreSQL Table

Run the SQLAlchemy model to create the `decoded_solana_transactions` table in your PostgreSQL database.

```bash
python -m backend.db.models
```

### 7. Run the Pipeline Components (Each in a New Terminal)

Ensure your Python virtual environment is activated in each terminal (`source venv/bin/activate`).

#### Terminal 1: Solana Log Producer
Streams raw Solana `logsNotification` messages from the Devnet WebSocket to Kafka.
```bash
python -m backend.services.solana
```

#### Terminal 2: Solana Log Processor
Consumes raw messages from Kafka, decodes them, and writes structured data to PostgreSQL.
```bash
python -m backend.services.processor
```

#### Terminal 3: FastAPI Backend
Serves processed data from PostgreSQL via API endpoints. Access Swagger UI at `http://localhost:8000/docs`.
```bash
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

#### Terminal 4: Streamlit Dashboard
Connects to the FastAPI backend and visualizes the data.
```bash
streamlit run frontend/dashboard.py
```
Open your browser to the URL provided by Streamlit (usually `http://localhost:8501`).

### 8. Verify Data in PostgreSQL (Optional)

You can connect to your PostgreSQL container and query the database:
```bash
docker exec -it postgres_db psql -U user -d solana_liquidity
```
Then, at the `psql` prompt:
```sql
SELECT id, transaction_signature, program_id, event_type FROM decoded_solana_transactions LIMIT 10;
SELECT COUNT(*) FROM decoded_solana_transactions;
\q
```

## Uses and Value Proposition

This MVP project demonstrates a robust, real-time data engineering pipeline for blockchain data. It lays the groundwork for:

*   **Real-time Blockchain Analytics:** Monitoring on-chain activity as it happens.
*   **DeFi Liquidity Sniping:** The ultimate goal is to identify high-value liquidity events (e.g., new liquidity pool creations, large liquidity additions/removals, significant token swaps) that can precede market movements.
*   **Quantitative Trading Strategy Development:** Providing structured data for backtesting and developing trading bots.
*   **Protocol Monitoring:** Gaining insight into the activity of specific smart contracts or dApps on Solana.

It showcases the ability to turn complex, high-volume, unstructured blockchain logs into actionable, structured data, a core skill in blockchain data engineering and analogous to services like Dune Analytics.

## Next Steps & Future Enhancements

To evolve this MVP into a fully-fledged "Solana Liquidity Sniper" tool, the following enhancements are critical:

1.  **Enhanced `SolanaLogDecoder`:**
    *   **Protocol-Specific Decoding:** Implement detailed parsing logic for specific Solana programs (e.g., SPL Token program, Raydium, Orca, Jupiter).
    *   **Extract Granular Details:** Extract token mint addresses, exact amounts, liquidity pool IDs, and precise event types (e.g., "Swap USDC->SOL", "Add Liquidity RAY/USDC"). This is the **most crucial** next step to unlock true "sniper" functionality.
2.  **Price Oracle Integration:**
    *   Integrate with real-time price feeds (e.g., Pyth Network or market data APIs) to augment transaction data with token prices. This is necessary for identifying arbitrage opportunities.
3.  **Arbitrage/Opportunity Detection Logic:**
    *   Develop a dedicated service or integrate logic into the processor/FastAPI to compare prices across different DEXes or identify specific high-value events based on decoded details.
4.  **Alerting System:**
    *   Implement notification mechanisms (e.g., Discord, Telegram, email) for detected opportunities or significant events.
5.  **Dashboard Filtering & Interactivity:**
    *   Add filters for `program_id`, `event_type`, `token_mint_address`, etc.
    *   Implement detailed views for individual transactions (fetching `raw_log_message`).
6.  **Scalability & Performance Optimizations:**
    *   Optimize database queries, potentially implementing caching.
    *   Scale Kafka and PostgreSQL for higher throughput and storage.
7.  **Production Deployment:**
    *   Containerize each service with specific Dockerfiles.
    *   Deploy to a cloud provider (e.g., AWS ECS/Fargate, MSK, RDS) with CI/CD pipelines.
    *   Implement robust monitoring, logging, and error handling.
    *   Switch to a reliable, high-performance Solana RPC provider with an API key.

---
**Happy Sniping!**
