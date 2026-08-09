import os
import chromadb
import chromadb.utils.embedding_functions as embedding_functions
from dotenv import load_dotenv
from openai import AzureOpenAI

load_dotenv()
COLLECTION_NAME=os.getenv("CHROMADB_COLLECTION_NAME", "knowledge-base")
TOP_K=3

SYSTEM_PROMPT = (
    "You are a world class TM8 Bank knowledge assistant.\n" \
    "Answer only from the retrieved context. Keep the answer concise and factual.\n"
    "If the context does not contain the answer, say:"
    "I don\'t know based on the provided documents."
)

def get_collection():
    client = chromadb.HttpClient(
        host = os.getenv("CHROMADB_HOST", "localhost"),
        port = int(os.getenv("CHROMADB_PORT", "8000")),
    )
    openai_ef = embedding_functions.OpenAIEmbeddingFunction(
        model_name=os.getenv("AZURE_OPENAI_API_EMBEDDING_MODEL", "text-embedding-3-large"),
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_base=os.getenv("AZURE_OPENAI_API_ENDPOINT"),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        deployment_id=os.getenv("AZURE_OPENAI_API_EMBEDDING_MODEL"),
        api_type="azure",
    )
    return client.get_collection(COLLECTION_NAME, embedding_function=openai_ef)

def get_llm_client() -> AzureOpenAI:
    return AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        azure_endpoint=os.getenv("AZURE_OPENAI_API_ENDPOINT"),
    )

def retrieve(question: str, top_k: int = TOP_K) -> tuple[list[str], list[str]]:
    collection = get_collection()
    result = collection.query(query_texts=[question], n_results=top_k)
    documents = result["documents"][0]
    sources = [metadata["source_file"] for metadata in result["metadatas"][0]]
    return documents, list(dict.fromkeys(sources))

def generate(question:str, documents:list[str]) -> str:
    context = "\n\n<---------->\n\n".join(documents)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Retrieved context:\n{context}\n\nQuestion: {question}"},
    ]
    llm_client = get_llm_client()
    response = llm_client.chat.completions.create(
        model = os.getenv("AZURE_OPENAI_API_CHAT_DEPLOYMENT"),
        messages = messages,
    )
    return response.choices[0].message.content