import os
import uuid

from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import dagster_client
import postgres_rag
import rag

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
PDF_UPLOAD_DIR = os.getenv(
    "PIPELINES_PDF_DIR",
    os.path.join(BACKEND_DIR, "..", "pipelines", "data", "capstone_part_2"),
)
CSV_UPLOAD_DIR = os.getenv(
    "PIPELINES_CSV_DIR",
    os.path.join(BACKEND_DIR, "..", "pipelines", "data", "capstone_part_1"),
)
# CSV uploads land here, not directly at CSV_UPLOAD_DIR -- keeps the original files untouched
CSV_UPLOAD_SUBDIR = "uploads"

# CSV uploads are always a fresh copy of one of these two known files
CSV_ASSET_BY_FILENAME = {
    "core_customers.csv": (["retrieve_raw_customers", "silver_customers", "gold_customers"], "retrieve_raw_customers"),
    "crm_contacts.csv": (["retrieve_raw_crm_contacts", "silver_customers", "gold_customers"], "retrieve_raw_crm_contacts"),
}

app = FastAPI(title="RAG Chatbot API")

# Setup CORS to allow Streamlit to talk to FastAPI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    query: str
    data_source: str
    history: list[dict] = []

class ChatResponse(BaseModel):
    answer: str
    source_used: str

class UploadResponse(BaseModel):
    status: str
    message: str

@app.get("/health")
def health_check():
    return {"status": "healthy", "message": "FastAPI is running"}

@app.post("/upload", response_model=UploadResponse)
def upload_endpoint(file: UploadFile = File(...)):
    filename = os.path.basename(file.filename)
    content = file.file.read()

    if filename.lower().endswith(".pdf"):
        dest_path = os.path.join(PDF_UPLOAD_DIR, filename)
        asset_selection = ["chromadb_status", "ingest_products"]
        run_config = {"ops": {"ingest_products": {"config": {"target_file": filename}}}}

    elif filename.lower().endswith(".csv"):
        match = CSV_ASSET_BY_FILENAME.get(filename)
        if match is None:
            return UploadResponse(
                status="failed",
                message="CSV uploads must be named exactly 'core_customers.csv' or 'crm_contacts.csv'.",
            )
        asset_selection, target_asset = match

        upload_dir = os.path.join(CSV_UPLOAD_DIR, CSV_UPLOAD_SUBDIR)
        os.makedirs(upload_dir, exist_ok=True)
        stored_name = f"{uuid.uuid4().hex[:8]}_{filename}"
        dest_path = os.path.join(upload_dir, stored_name)
        target_file = f"{CSV_UPLOAD_SUBDIR}/{stored_name}"
        run_config = {"ops": {target_asset: {"config": {"target_file": target_file}}}}

    else:
        return UploadResponse(status="failed", message="Only PDF or CSV uploads are supported.")

    with open(dest_path, "wb") as dest:
        dest.write(content)

    try:
        run_id = dagster_client.trigger_materialization(asset_selection, run_config)
        succeeded = dagster_client.wait_for_run(run_id, timeout=120)
    except Exception as e:
        return UploadResponse(status="failed", message=f"Failed to process {filename}: {e}")

    if not succeeded:
        return UploadResponse(status="failed", message=f"Dagster run failed while processing {filename}.")

    return UploadResponse(status="success", message=f"{filename} processed and ready to query.")

def _respond(query: str, documents: list[str], sources: list[str], history: list[dict]) -> ChatResponse:
    if not documents:
        answer = "I don't know based on the provided documents."
    else:
        answer = rag.generate(query, documents, history=history)
    source_used = ", ".join(sources) if sources else "none"
    return ChatResponse(answer=answer, source_used=source_used)


@app.post("/chat", response_model=ChatResponse)
def chat_endpoint(request: ChatRequest):
    history_text = " ".join(turn["content"] for turn in request.history)
    search_query = f"{history_text} {request.query}".strip()

    if request.data_source == "chroma":
        documents, sources = rag.retrieve(search_query)
    elif request.data_source == "postgres":
        documents, sources = postgres_rag.retrieve(search_query)
    else:
        chroma_documents, chroma_sources = rag.retrieve(search_query)
        postgres_documents, postgres_sources = postgres_rag.retrieve(search_query)
        documents, sources = chroma_documents + postgres_documents, chroma_sources + postgres_sources
    return _respond(request.query, documents, sources, request.history)