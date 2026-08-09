from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import rag, postgres_rag

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

class ChatResponse(BaseModel):
    answer: str
    source_used: str

@app.get("/health")
def health_check():
    return {"status": "healthy", "message": "FastAPI is running"}

def respond(query: str, documents: list[str], sources:list[str]) -> ChatResponse:
    if not documents:
        answer = "I don't know based on the provided documents"
    else:
        answer = rag.generate(query, documents)
    source_used = ", ".join(sources) if sources else "none"
    return ChatResponse(answer=answer, source_used=source_used)

@app.post("/chat", response_model=ChatResponse)
def chat_endpoint(request: ChatRequest):
    if request.data_source == "chroma":
        documents, sources = rag.retrieve(request.query)
    elif request.data_source == "postgres":
        documents, sources = postgres_rag.retrieve(request.query)
    else:
        chroma_documents, chroma_sources = rag.retrieve(request.query)
        postgres_documents, postgres_sources = postgres_rag.retrieve(request.query)
        documents, sources = chroma_documents + postgres_documents, chroma_sources + postgres_sources
    return respond(request.query, documents, sources)