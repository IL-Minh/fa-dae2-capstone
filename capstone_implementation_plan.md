Implementation plan

Scope: stand up a local data stack with synthetic realtime and batch pipelines, Postgres sink, optional Airflow → Snowflake, dbt transforms, DuckDB previews, and AI agent later.

Phase 0 — prerequisites
- Ensure Docker Desktop is installed and running; use `docker compose` (V2).
- Create `.env` with Postgres values: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT`.
- Verify `.env` is gitignored (already is).

Phase 1 — Postgres baseline (done/verify)
- Pin image to `postgres:17.4` in `docker-compose.yml`.
- Keep named volume `postgres_data:/var/lib/postgresql/data`.
- Healthcheck with `pg_isready` (present).
- Start DB: `docker compose up -d` and connect with DBeaver.
- Smoke test: create a table; restart container; verify persistence.

Phase 2 — repo scaffolding
- Create folders: `infra/`, `producers/`, `consumers/`, `loaders/`, `snowflake/dbt_project/`, `ai_agent/`, `docs/`.
- Add `pyproject.toml` deps (core): `python-dotenv>=1.0.1`, `SQLAlchemy~=2.0`, `psycopg2-binary~=2.9`, `pandas~=2.2`.
- Optional deps per feature will be added in their submodules later.

Phase 3 — realtime pipeline (optional to start)
- `producers/kafka_producer_faker.py`: emit synthetic `transactions` using Faker to Kafka.
- `consumers/kafka_to_postgres.py`: consume from Kafka, `CREATE TABLE IF NOT EXISTS transactions_sink`, upsert rows.
- Compose: extend `infra/dev.compose.yml` to include Bitnami Zookeeper+Kafka alongside Postgres.
- Test: run producer and consumer; verify rows in Postgres.

Phase 4 — batch pipeline (synthetic, no personal data)
- `loaders/generate_monthly.py`: generate monthly CSV and small sample PDFs into `data/incoming/`.
- `loaders/csv_pdf_ingest.py`: parse CSV/PDF (pdfplumber/PyMuPDF) and load to Postgres tables.
- Test: generate files; run ingest; verify tables populated.

Phase 5 — transforms & analytics
- Initialize `snowflake/dbt_project/` (dbt Core targeting Snowflake). Add profiles via env vars.
- Create staging models for sink tables, marts for analytics.
- Add `scripts/duckdb_preview.py` (or notebook) to pull a recent window from Postgres into DuckDB for quick analysis.

Phase 6 — orchestration (when ready)
- Add `airflow/docker-compose.yml` using official images; mount `airflow/dags/`.
- DAG `airflow/dags/load_postgres_to_snowflake.py`: incremental loads based on watermark.
- Configure Airflow connection/env via `.env` or UI; avoid committing secrets.

Phase 7 — AI agent (optional later)
- `ai_agent/embed_and_index.py`: build embeddings from Snowflake (or CSV) into Chroma.
- `ai_agent/agent_cli.py`: LangGraph agent that queries Snowflake for facts and Chroma for semantic search.

Phase 8 — docs & DX
- Update `README.md` with quickstart per feature (DB, Kafka, batch, dbt, Airflow, agent).
- Add `Makefile` targets: `make up`, `make down`, `make logs`, `make gen-batch`, `make ingest-batch`, etc.

Operational notes
- Use pinned versions (e.g., Postgres 17.4) for stability.
- Prefer named volumes in Docker for database persistence.
- Keep all credentials in `.env` and never in VCS.
- Use DBeaver 25.0.2 for DB exploration.


