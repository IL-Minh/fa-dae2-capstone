Design overview

Goal: local dev stack that ingests realtime transactions (simulated with Faker → Kafka) and batch synthetic files, lands in Postgres (staging/streaming sink), periodically ships new rows to Snowflake (source of truth), transforms with dbt Core (Snowflake target), supports quick analytics in DuckDB, and exposes an AI agent (LangGraph + LLM + local vector DB) as CLI or Streamlit.

Data policy: Only synthetic data is used in both realtime and batch pipelines. No personal data.

High-level flows
- Realtime: Faker producer → Kafka `transactions` → consumer → Postgres sink → optional Airflow DAG → Snowflake → dbt models → analytics.
- Batch: Generator produces monthly synthetic CSV/PDF → folder `data/incoming/` → ingestion script → Postgres (or direct Snowflake) → dbt models.

Components & versions (current)
- Python 3.13, Git, Docker Desktop, Cursor/VS Code
- Postgres (Docker): postgres:17.4 (pinned)
- DBeaver client: 25.0.2
- DuckDB (local analytics): ~1.0
- dbt Core (Snowflake target) — add when Snowflake is ready
- Kafka (Bitnami images) — add for realtime
- Airflow (official images) — add when orchestration is needed

Libraries (baseline)
- Core/runtime: python-dotenv (>=1.0.1), SQLAlchemy (~2.0), psycopg[binary] (~3.2)
- DataFrames: polars (>=1.2) with connectorx for fast DB IO
- Streaming (later): confluent-kafka (~2.4), faker (~25)
- Batch docs (later): pdfplumber (~0.11), PyMuPDF (~1.24)
- Optional: snowflake-connector-python (~3.16), chromadb (~0.5), sentence-transformers (~3.0)

Suggested repo layout
personal-finance-project/
├─ infra/
│  └─ docker-compose.yml (dev compose)
├─ producers/
│  └─ kafka_producer_faker.py
├─ consumers/
│  └─ kafka_to_postgres.py
├─ airflow/  (optional later)
│  └─ dags/
│     └─ load_postgres_to_snowflake.py
├─ loaders/
│  ├─ generate_monthly.py
│  └─ csv_pdf_ingest.py
├─ snowflake/
│  └─ dbt_project/
├─ ai_agent/
│  ├─ langgraph_config.yaml
│  ├─ embed_and_index.py
│  └─ agent_cli.py
└─ docs/

Notes
- Use Docker named volumes for Postgres data (`postgres_data:/var/lib/postgresql/data`).
- Keep credentials in `.env`; never commit secrets.
- Prefer `docker compose` (V2) commands.