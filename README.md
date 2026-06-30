# Real-Time Financial Transaction Pipeline

A production-grade real-time data pipeline simulating financial transaction processing using modern data engineering tools.

## Architecture
Kafka Producer → Apache Kafka → Kafka Consumer → Snowflake → dbt → Looker Studio
↑
Airflow (orchestration)

## Tech Stack
| Layer | Tool |
|-------|------|
| Streaming | Apache Kafka + Zookeeper |
| Ingestion | Python, kafka-python |
| Data Warehouse | Snowflake (AWS us-east-2) |
| Transformation | dbt (staging + mart models) |
| Orchestration | Apache Airflow |
| Containerization | Docker |
| Dashboard | Looker Studio |

## Results
- Real-time transaction ingestion at 1 event/second
- 459+ transactions loaded into Snowflake RAW_TRANSACTIONS table
- dbt models: stg_transactions (view) + mart_transactions (aggregated table)
- Airflow DAG scheduled hourly for dbt runs

## Setup
```bash
git clone https://github.com/surendrasappa08/fintech-pipeline
cd fintech-pipeline
cp .env.example .env  # fill in your Snowflake credentials
docker compose up -d
python3 producer/transaction_producer.py
python3 consumer/snowflake_consumer.py
```

## Project Structure
fintech-pipeline/
├── producer/          # Kafka transaction generator
├── consumer/          # Snowflake ingestion consumer
├── dbt/fintech_dbt/   # dbt models (staging + mart)
├── airflow/dags/      # Airflow DAG
├── docker-compose.yml # Kafka + Zookeeper
└── .env.example       # credentials template
## Dashboard
[Fintech Real-Time Transaction Dashboard](https://datastudio.google.com/u/1/reporting/d4a7ed12-135a-41c6-bc20-08ea68188ec)
