# Frontend

Streamlit chat UI for the RAG chatbot. Lets you pick a data source (Postgres customer data, ChromaDB product docs, or both), chat against it, and optionally upload new documents into the knowledge base.

## What it does

- Sends chat turns to the backend's `POST /chat`, passing the selected data source and recent history.
- Renders the "no relevant context found" case distinctly from a normal answer, and shows the `source_used` the backend reports.
- Lets you upload a PDF or CSV via `POST /upload`, which triggers a Dagster ingestion run on the backend and blocks (with a spinner) until it finishes.
- Chat history renders inside a fixed-height scrolling container, with the data-source toggles above it — both stay in place as the conversation grows, instead of scrolling away.

## Configuration

| Env var | Default | Purpose |
| --- | --- | --- |
| `API_URL` | `http://localhost:8001` | Base URL of the backend FastAPI service |

## Local development

```bash
cd frontend
uv sync
uv run streamlit run main.py
```

By default this expects the backend to be reachable at `http://localhost:8001` — either run `backend/` locally too (see [`../backend/README.md`](../backend/README.md)), or export `API_URL` to point at wherever it's actually running.

## Running via Docker Compose

This service normally isn't run standalone — it's one of five containers brought up together from the repo root:

```bash
cd ..
cp .env.sample .env   # fill in the values .env.sample documents
docker compose up -d
```

See the [root README](../README.md) for the full stack, or [`../infra/README.md`](../infra/README.md) for deploying it to Azure.