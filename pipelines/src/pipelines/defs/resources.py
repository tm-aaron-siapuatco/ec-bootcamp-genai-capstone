import dagster as dg
import pandas as pd
import os
from sqlalchemy import create_engine
import chromadb
from chromadb.api import ClientAPI
import chromadb.utils.embedding_functions as embedding_functions
class DataResources(dg.ConfigurableResource):
    """Data Resource"""
    base_directory: str = "data"
    
    def read_csv(self, filename: str):
        filepath = os.path.join(self.base_directory,"capstone_part_1", filename)
        if not filepath:
            raise FileNotFoundError(f"CSV not found!: {filepath}")
        return pd.read_csv(filepath)

    def get_pdf_files(self) -> list[str]:
        """Return all PDF file paths from the product documents directory."""
        filepath = os.path.join(self.base_directory, "capstone_part_2")

        if not os.path.isdir(filepath):
            raise FileNotFoundError(f"PDF directory not found: {filepath}")

        return sorted(
            os.path.join(filepath, filename)
            for filename in os.listdir(filepath)
            if filename.lower().endswith(".pdf")
        )

class PostgresResource(dg.ConfigurableResource):
    """Postgres Resource"""
    def get_engine(self):
        user = os.getenv("DATABASE_USER", "postgres")
        host = os.getenv("DATABASE_HOST", "localhost")
        name = os.getenv("DATABASE_NAME", "postgres")
        password = os.getenv("DATABASE_PASSWORD", "password")
        port = os.getenv("DATABASE_PORT", "5432")

        db_uri = f"postgresql://{user}:{password}@{host}:{port}/{name}"
        return create_engine(db_uri)

class ChromaDBResource(dg.ConfigurableResource):
    """ChromaDB resource connection."""
    directory: str = "./chroma_db_data"

    def get_client(self) -> ClientAPI:
        return chromadb.HttpClient(
            host=os.getenv("CHROMADB_HOST", "localhost"),
            port=int(os.getenv("CHROMADB_PORT", "8000"))
        )

    def embedding_function(self):
        return embedding_functions.OpenAIEmbeddingFunction(
                model_name=os.getenv("EMBEDDING_MODEL", "text-embedding-3-large"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                api_base=os.getenv("AZURE_OPENAI_API_ENDPOINT"),
                api_version="2024-10-21",
                deployment_id=os.getenv("EMBEDDING_MODEL"),
                api_type="azure",
            )
