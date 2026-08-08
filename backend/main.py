from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

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
    #TODO: Psuedocode
    if request.data_source == "postgres":
        response_text = f"Querying Gold Table for: {request.query}"
    elif request.data_source == "chroma":
        response_text = f"Searching Vector Store for: {request.query}"
    else:
        response_text = f"Querying BOTH for: {request.query}"
        
    return ChatResponse(answer=response_text, source_used=request.data_source)