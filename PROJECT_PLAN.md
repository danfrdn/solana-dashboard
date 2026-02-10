# Solana Liquidity Sniper Dashboard MVP - Project Plan

## Objective
Develop a Minimum Viable Product (MVP) for a Solana Liquidity Sniper Dashboard. This dashboard will demonstrate real-time data ingestion, decoding of Solana blockchain transactions (specifically for liquidity pool events), and interactive data visualization using Python, Helius RPC, and Streamlit. The goal is to showcase proficiency in data engineering principles relevant to high-throughput, real-time data pipelines.

## Core Principles
*   **Modularity:** Keep components loosely coupled for reusability and maintainability.
*   **Performance:** Optimize data ingestion and processing for near real-time updates.
*   **Clean Schemas:** Design data structures that are intuitive, efficient for storage, and easy to query.
*   **Dune-Style:** Focus on clear, query-friendly data representations.
*   **Scalability & Robustness:** Utilize message queues (Kafka) and cloud-native services (AWS) for resilient and scalable architecture.

## High-Level Technical Milestones

### Milestone 1: Setup & Raw Data Ingestion to Kafka
*   **Goal:** Establish basic project environment, set up local Docker Compose for Kafka & PostgreSQL, connect to Helius RPC, and publish raw transaction data to a Kafka topic.
*   **Key Activities:**
    *   Project scaffolding with defined directory structure.
    *   `docker-compose.yml` for local Kafka broker and PostgreSQL database.
    *   Setup Helius RPC client and WebSocket connection in `backend/services/helius.py`.
    *   Implement Kafka producer in `backend/services/helius.py` to publish raw transaction messages to a `solana_raw_transactions` Kafka topic.

### Milestone 2: Kafka Consumption, Transaction Decoding & DB Storage
*   **Goal:** Consume raw transactions from Kafka, decode Solana instructions, extract liquidity event features, and persist structured data into PostgreSQL.
*   **Key Activities:**
    *   Implement Kafka consumer in `backend/services/kafka_consumer.py` to read from `solana_raw_transactions`.
    *   Develop `backend/services/transaction_decoder.py` for parsing Solana instructions (e.g., for Raydium, Orca Program IDs) and extracting relevant liquidity-related data (token addresses, amounts, timestamps, event types).
    *   Define SQLAlchemy ORM models in `backend/db/models.py` for our structured liquidity events.
    *   Implement logic within the Kafka consumer to write the decoded and structured events to the PostgreSQL database.

### Milestone 3: FastAPI Backend & Data API
*   **Goal:** Develop a FastAPI application to expose the processed liquidity data via a clean, performant REST API.
*   **Key Activities:**
    *   FastAPI application setup in `backend/main.py`.
    *   Define Pydantic schemas in `backend/schemas/solana.py` for API request/response validation and serialization.
    *   Implement API endpoints in `backend/api/v1/liquidity.py` to query the PostgreSQL database (e.g., for recent swaps, liquidity pool status, token information).

### Milestone 4: Streamlit Dashboard & Real-time Visualization
*   **Goal:** Build an interactive Streamlit application to visualize the processed liquidity data, fetching it from the FastAPI backend.
*   **Key Activities:**
    *   Streamlit application setup in `frontend/app.py`.
    *   Create `frontend/services/api_client.py` to handle interactions with the FastAPI backend.
    *   Design and implement key visualizations (e.g., recent swaps table, liquidity change charts, token pair details) using Streamlit components and charting libraries.
    *   Implement dashboard auto-refresh or polling mechanisms to display near real-time data updates.

### Milestone 5: Monitoring, Error Handling, and Deployment Considerations (AWS)
*   **Goal:** Implement basic monitoring, robust error handling, and outline steps for potential AWS deployment.
*   **Key Activities:**
    *   Integrate structured logging throughout the backend services.
    *   Implement basic health checks for FastAPI and Kafka consumers.
    *   Discuss and document AWS deployment strategy (e.g., EC2 for FastAPI/Streamlit, RDS for PostgreSQL, MSK for Kafka, IAM roles, VPC configuration).
    *   Refine `Dockerfile` for containerization of backend and frontend services.

---

## Application Process Diagram (Textual Description)

Here's a textual representation of the application process flow:

```
+-------------------+     +-----------------------------------+
| Helius RPC        |     | Backend: Helius WebSocket Client  |
| (Solana Data)     | --> | (services/helius.py)              |
+-------------------+     +-----------------------------------+
                                         |
                                         V
                              +--------------------------+
                              | Backend: Kafka Producer  |
                              | (services/helius.py)     |
                              +--------------------------+
                                         |
                                         V
+-------------------------------------------------------------+
| Kafka Topic: solana_raw_transactions                        |
| (Acts as a robust message queue for raw Solana data)        |
+-------------------------------------------------------------+
                                         |
                                         V
                              +--------------------------+
                              | Backend: Kafka Consumer  |
                              | (services/kafka_consumer.py) |
                              +--------------------------+
                                         |
                                         V
                          +-------------------------------+
                          | Backend: Transaction Decoder  |
                          | (services/transaction_decoder.py) |
                          | (Parses Solana instructions,  |
                          |  extracts liquidity events)   |
                          +-------------------------------+
                                         |
                                         V
                         +-----------------------------------+
                         | Backend: Database Writer          |
                         | (db/database.py, db/models.py)    |
                         | (Persists decoded events to DB)   |
                         +-----------------------------------+
                                         |
                                         V
+-------------------------------------------------------------+
| PostgreSQL Database (e.g., AWS RDS)                         |
| (Stores clean, structured liquidity event data)             |
+-------------------------------------------------------------+
                                         ^
                                         |
+-----------------------------------+    |
| Streamlit Frontend (app.py)       |    |
| (api_client.py fetches data)      | <--|
+-----------------------------------+    |
                                         |
                                         V
                         +-----------------------------------+
                         | FastAPI Backend (main.py)         |
                         | (api/v1/liquidity.py endpoints)   |
                         | (Queries DB, serves data to UI)   |
                         +-----------------------------------+
```
