from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import rag

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

@app.post("/chat", response_model=ChatResponse)
def chat_endpoint(request: ChatRequest):
    if request.data_source == "chroma":
        documents, sources = rag.retrieve(request.query)
        if not documents:
            answer = "I don't know based on the provided documents."
        else:
            answer = rag.generate(request.query, documents)
        source_used = ", ".join(sources) if sources else "none"
    elif request.data_source == "postgres":
        # text_to_sql?
        answer = "idk"
    else:
        answer = f"Querying BOTH for: {request.query}"
        source_used = "both"

    return ChatResponse(answer=answer, source_used=source_used)