# Backend

FastAPI service that powers the RAG chatbot: retrieves context from ChromaDB and/or Postgres, calls Azure OpenAI to generate an answer, and triggers Dagster ingestion runs for uploaded documents.

## Endpoints

- `GET /health` — liveness check.
- `POST /chat` — `{query, data_source, history}` → `{answer, source_used}`. `data_source` is `"chroma"`, `"postgres"`, or anything else (treated as "both" — results from each are merged).
- `POST /upload` — accepts a PDF or CSV file. PDFs are saved and materialize the `chromadb_status` + `ingest_products` Dagster assets (scoped to that file). CSVs must be named exactly `core_customers.csv` or `crm_contacts.csv` and materialize the matching bronze → silver → gold asset chain. Blocks (via `dagster_client.wait_for_run`) until the Dagster run finishes or times out (120s).

## How retrieval works

- **`rag.py`** — ChromaDB path. Embeds the query, queries the `knowledge_base` collection (product PDF chunks), and generates an answer via Azure OpenAI chat completions. Returns the fallback "I don't know based on the provided documents." when there's no relevant context.
- **`postgres_rag.py`** — Postgres path. No embeddings involved: it pattern-matches the question for an email, a `CUST-<digits>` customer ID, or a first/last name pair, then queries the `gold_customers` table directly. Returns nothing if none of those patterns match.
- **`dagster_client.py`** — talks to `dagster-webserver`'s GraphQL API to launch asset materializations and poll run status; used by `/upload`, and separately by the CI/CD deploy step to auto-ingest on every deploy (see [`../infra/README.md`](../infra/README.md)).

## Configuration

All of these have defaults suitable for the Docker Compose stack (see [`docker-compose.yml`](../docker-compose.yml)); override via environment variables for standalone local runs.

| Env var | Default | Purpose |
| --- | --- | --- |
| `DATABASE_HOST` / `DATABASE_PORT` / `DATABASE_USER` / `DATABASE_PASSWORD` / `DATABASE_NAME` | `localhost` / `5432` / `postgres` / `password` / `postgres` | Postgres connection for `postgres_rag.py` |
| `CHROMADB_HOST` / `CHROMADB_PORT` | — (required) | ChromaDB connection |
| `CHROMADB_COLLECTION_NAME` | `knowledge_base` | Collection queried/populated for document chunks |
| `DAGSTER_GRAPHQL_URL` | `http://localhost:3000/graphql` | Dagster webserver GraphQL endpoint |
| `PIPELINES_PDF_DIR` | `../pipelines/data/capstone_part_2` | Where uploaded PDFs are written |
| `PIPELINES_CSV_DIR` | `../pipelines/data/capstone_part_1` | Where uploaded CSVs are written (under an `uploads/` subfolder) |
| `AZURE_OPENAI_API_KEY` / `AZURE_OPENAI_API_ENDPOINT` / `AZURE_OPENAI_API_VERSION` / `AZURE_OPENAI_API_CHAT_DEPLOYMENT` / `AZURE_OPENAI_API_EMBEDDING_MODEL` | — (required) | Azure OpenAI credentials for chat + embeddings |

## Local development

```bash
cd backend
uv sync
uv run uvicorn main:app --reload --port 8001
```

Needs Postgres, ChromaDB, and `dagster-webserver` reachable at whatever the env vars above point to — either run those via `docker compose up postgres chromadb dagster-webserver` from the repo root, or point at already-running instances.

## Running via Docker Compose

Not normally run standalone — see the [root README](../README.md) for bringing up the full 5-container stack, or [`../infra/README.md`](../infra/README.md) for deploying it to Azure.