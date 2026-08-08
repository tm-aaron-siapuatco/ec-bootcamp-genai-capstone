import pytest
from unittest.mock import MagicMock
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool
from dagster import build_asset_context

@pytest.fixture
def mock_data_resource():
    """
    Simulates the DataResources class.
    Instead of reading from the real file system, it hands the pipeline 
    two fake file paths to process.
    """
    mock_data = MagicMock()
    mock_data.get_pdf_files.return_value = [
        "/fake/path/product_A.pdf", 
        "/fake/path/product_B.pdf"
    ]
    return mock_data


@pytest.fixture
def mock_chroma_resource():
    """
    Simulates the ChromaDBResource, the Client, and the Collection all at once.
    """
    # 1. Create the top-level resource mock
    mock_resource = MagicMock()
    
    # 2. Create the mock client that gets returned when get_client() is called
    mock_client = MagicMock()
    mock_resource.get_client.return_value = mock_client
    
    # Simulate a successful heartbeat (no errors raised)
    mock_client.heartbeat.return_value = None 
    
    # 3. Create the mock collection that gets returned by the client
    mock_collection = MagicMock()
    mock_client.get_or_create_collection.return_value = mock_collection
    mock_client.get_collection.return_value = mock_collection
    
    # Simulate database responses for your asset check
    mock_collection.count.return_value = 10 
    mock_collection.get.return_value = {
        "metadatas": [
            {"source_file": "product_A.pdf"}, 
            {"source_file": "product_B.pdf"}
        ]
    }
    
    # 4. Mock the Azure OpenAI embedding function
    mock_resource.embedding_function.return_value = MagicMock()
    
    return mock_resource


@pytest.fixture
def mock_context():
    """
    Provides a standard Dagster asset context for testing so you 
    don't have to rebuild it in every single test file.
    """
    return build_asset_context()

@pytest.fixture
def test_postgres_resource():
    # pool_reset_on_return=None stops the nasty SQLite teardown traceback spam
    engine = create_engine(
        "sqlite:///:memory:", 
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
        pool_reset_on_return=None 
    )
    
    # Trick SQLite into accepting schema="public" from your production code
    with engine.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS public"))
    
    class MockPostgresResource:
        def get_engine(self):
            return engine
            
    return MockPostgresResource()