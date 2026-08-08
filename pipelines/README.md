# Medallion Pipelines and Document Ingestion 

This directory contains the Dagster-based data and retrieval pipeline for the capstone project. It complements the root project README by focusing on the operational logic that runs the pipeline locally: ingesting source files, cleaning and combining customer data through a medallion architecture, and preparing documents for vector search in ChromaDB.

## What this pipeline is doing

The project combines three main components:

- Dagster orchestrates the workflow and materializes the assets.
- PostgreSQL stores the transformed tabular data in bronze, silver, and gold layers.
- ChromaDB stores embedded document chunks generated from product PDFs for retrieval-based use cases.

The overall design follows the same thinking described in the project overview: we want a reliable path from raw source files to business-ready outputs, while keeping enough quality checks in place that broken or malformed data does not silently flow downstream.

## Medallion architecture and transformation thinking

The transformation stages are intentionally separated so each layer has a clear responsibility.

### Bronze: preserve raw truth and surface data issues early

The bronze layer is the first landing point for the raw customer and CRM datasets. The goal here is not to be overly clever; it is to preserve the source data as closely as possible while recording basic quality signals.

In practice, the bronze assets:

- load the raw CSV files into PostgreSQL as bronze tables,
- capture row counts and null distributions for observability,
- run lightweight checks for non-empty input and basic data quality rules such as valid email formatting and the presence of required columns.

This layer is intentionally simple because the most important job of bronze is to make sure we get the data as it is, before any cleanup or joining begins.

### Silver: normalize, merge, and deduplicate

The silver layer is where the customer data becomes usable. The logic here focuses on making the two source datasets compatible and removing obvious quality issues before they reach downstream analytics.

The transformations in silver include:

- creating a combined full name from first/middle/last name fields,
- normalizing email addresses and phone numbers,
- standardizing field names across the core customer and CRM contact datasets,
- converting balance and date fields into consistent formats and proper types,
- resolving duplicate records by email,
- merging the two sources using email as the join key,
- dropping rows that are missing critical business identifiers.

The thinking here is that the silver layer should become a dependable customer model. When I was looking at the csv I noticed a lot of small errors that could potentially decrease the quality of the data. Hence, it should be a reliable dataset that is less vulnerable to inconsistent source schemas, and ready for more analytical use.

### Gold: create business-ready features and segmentation

For the gold layer I noticed this is where we add derived values where the data becomes decision-oriented rather than merely cleaned, while adding business context too.

The gold transformations include:

- assigning value tiers based on relationship balance,
- assigning tenure tiers based on tenure months,
- deriving age from birthdate,
- calculating a completeness score across the row,
- filtering out impossible ages so the dataset stays realistic and usable.

Because the use case is designed for downstream reporting and segmentation, this is why the transformations are more analytical than the earlier stages. Bronze protects its completeness and makes it untoutched, silver creates a unified model, and gold turns that model into something that can actually inform business interpretation.

## Why the document chunking process matters

The document ingestion asset reads PDF files, extracts text, and stores chunked text in ChromaDB so it can be embedded and later retrieved. The chunking strategy is intentionally simple and similar to the Document Ingestion exercise, since we have no way of evaluating the response, until the GenAI phase, I opted for this in the meantime.

We split the extracted text into chunks of about 1000 characters with 200 characters of overlap. This approach was chosen because:

- it keeps each chunk small enough to be manageable for embedding and retrieval,
- it preserves context around section boundaries by overlapping adjacent chunks,
- it avoids losing meaning when a concept is split across a natural break in the document,
- it is easy to reason about and tune later if retrieval quality improves or changes.

The current approach is a reasonable starting point for a capstone pipeline. It is deterministic, explainable, and easy to adjust once we have the ability to evaluate the LLM response later in the RAG implementation.

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [uv](https://docs.astral.sh/uv/) is installed following their official documentation.

Create a virtual environment and install the required dependencies using sync:

```bash
uv sync
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| macOS | `source .venv/bin/activate` |
| Windows | `.venv\Scripts\activate` |

**Option 2: pip**

Install the Python dependencies with pip:

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| macOS | `source .venv/bin/activate` |
| Windows | `.venv\Scripts\activate` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running the pipeline locally

From the `pipelines` directory, follow these steps to start the Dagster pipeline locally.

1. Create your environment file from the sample config:

```bash
cp .env.sample .env
```

Review the values in `.env` and keep the defaults unless you need to change them.

2. Make sure Docker is installed and running, then start the PostgreSQL container:

```bash
docker run -d --name postgres-app -p 5432:5432 \
  -e POSTGRES_DB=app-db \
  -e POSTGRES_USER=adminuser \
  -e POSTGRES_PASSWORD=password \
  -e PGDATA=/var/lib/postgresql/data/pgdata \
  -v postgres-data:/var/lib/postgresql/data postgres:15-alpine
```

If the container already exists, you can start it with:

```bash
docker start postgres-app
```

3. Start ChromaDB in a separate terminal:

```bash
chroma run --path ./chroma_db_data
```

4. Install dependencies if needed:

```bash
uv sync
```

5. Start the Dagster pipeline using the provided startup script:

```bash
./start-dagster.sh
```

The script will:

- load variables from `.env`,
- verify the PostgreSQL container is running,
- start the Dagster UI.

Once it is running, open http://localhost:3000 in your browser to view the project.

## Testing and Validation

The pipeline includes asset checks for the medallion workflow and the document ingestion workflow. These checks help catch issues such as empty datasets, malformed emails, inconsistent phone formats, invalid birthdates, and empty ChromaDB ingestion results.

You can run the test suite from the `pipelines` directory with:

```bash
pytest
```

## Learn more

To learn more about this project and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
