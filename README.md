# Data Pipeline

Generates 5000 random orders every 10 minutes and converts all currencies to EUR hourly using Apache Airflow.

## Architecture

- **postgres-1**: Source database (orders)
- **postgres-2**: Target database (orders_eur) 
- **Airflow**: Orchestration and scheduling
- **Grafana**: Monitoring dashboards

## Setup

1. Get free API key from [OpenExchangeRate](https://openexchangerates.org/)
2. Copy environment template and add your API key:
   ```bash
   cp .env.example .env
   ```
   Edit `.env` and set your API key:
   ```
   OPENEXCHANGE_API_KEY=your_api_key_here
   ```
3. Start all services:
   ```bash
   docker-compose up -d
   ```

## Access

- **Airflow UI**: http://localhost:8080 (admin/admin123)
- **Grafana Dashboards**: http://localhost:3000 (admin/admin)
- **PostgreSQL 1**: localhost:5432
- **PostgreSQL 2**: localhost:5433

## Enable DAGs

In Airflow UI, enable these DAGs:
- `orders-generator` - Generates 5000 orders every 10 minutes
- `currency-converter` - Converts currencies to EUR every hour

## Data Flow

1. **Data Generation**: Creates random orders with various currencies
2. **Currency Conversion**: Fetches exchange rates and converts amounts to EUR
3. **Monitoring**: Track progress via Grafana dashboards