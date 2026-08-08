from unittest.mock import MagicMock, patch

import pandas as pd
from dagster import build_asset_context

from src.pipelines.defs.bronze_pipeline import (
    bronze_customer_email_format,
    bronze_customer_required_fields,
    retrieve_raw_crm_contacts,
    retrieve_raw_customers,
)
from src.pipelines.defs.document_ingestion import chromadb_status, ingest_products
from src.pipelines.defs.gold_pipeline import gold_customers
from src.pipelines.defs.silver_pipeline import check_phone_format, silver_customers
from src.pipelines.defs.document_ingestion import check_ingestion_counts_greater_than_zero
import dagster as dg
import pytest

# Medallion Architecture
def test_retrieve_raw_customers_writes_to_database_and_returns_dataframe():
    context = build_asset_context()
    context.add_output_metadata = MagicMock()

    data_resource = MagicMock()
    input_df = pd.DataFrame({"email": ["alice@example.com"], "first_name": ["Alice"]})
    data_resource.read_csv.return_value = input_df

    database = MagicMock()
    engine = MagicMock()
    database.get_engine.return_value = engine

    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        result = retrieve_raw_customers(context=context, data=data_resource, database=database)

    assert result.equals(input_df)
    mock_to_sql.assert_called_once_with(
        name="bronze_customers",
        con=engine,
        schema="public",
        if_exists="replace",
        index=False,
    )
    context.add_output_metadata.assert_called_once()


def test_retrieve_raw_crm_contacts_writes_to_database_and_returns_dataframe():
    context = build_asset_context()
    context.add_output_metadata = MagicMock()

    data_resource = MagicMock()
    input_df = pd.DataFrame({"email_address": ["bob@example.com"], "is_active": ["Y"]})
    data_resource.read_csv.return_value = input_df

    database = MagicMock()
    engine = MagicMock()
    database.get_engine.return_value = engine

    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        result = retrieve_raw_crm_contacts(context=context, data=data_resource, database=database)

    assert result.equals(input_df)
    mock_to_sql.assert_called_once_with(
        name="bronze_contacts",
        con=engine,
        schema="public",
        if_exists="replace",
        index=False,
    )
    context.add_output_metadata.assert_called_once()


def test_silver_customers_transforms_and_merges_customer_and_crm_data():
    context = build_asset_context()
    context.add_output_metadata = MagicMock()

    customer_df = pd.DataFrame(
        {
            "email": ["alpha@example.com", "beta@example.com"],
            "customer_id": [1, 2],
            "first_name": ["Alpha", "Beta"],
            "middle_name": [None, None],
            "last_name": ["One", "Two"],
            "birthdate": ["1990-01-01", "1995-02-02"],
            "total_relationship_balance": [1000, 2000],
        }
    )
    crm_df = pd.DataFrame(
        {
            "email": ["alpha@example.com", "gamma@example.com"],
            "email_address": ["alpha@example.com", "gamma@example.com"],
            "phone_local": ["09171234567", "09172345678"],
            "region": ["Metro Manila", "Cebu"],
            "zip_code": ["1000", "6000"],
            "country_code": ["PH", "PH"],
            "join_date": ["2020-05-01", "2021-03-01"],
            "kyc_level_text": ["Gold", "Silver"],
            "tier": ["A", "B"],
            "segment_name": ["VIP", "Regular"],
            "opt_in_marketing": ["Y", "N"],
            "is_active": ["Y", "N"],
            "total_relationship_balance": ["1,200", "2,500"],
            "average_daily_balance": ["800", "1,200"],
            "dob": ["1988-05-10", "1992-10-10"],
        }
    )

    database = MagicMock()
    database.get_engine.return_value = MagicMock()

    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        result = silver_customers(
            context=context,
            retrieve_raw_customers=customer_df,
            retrieve_raw_crm_contacts=crm_df,
            database=database,
        )

    assert "full_name" in result.columns
    assert "marketing_opt_in" in result.columns
    assert result.loc[result["email"] == "alpha@example.com", "status"].iloc[0] == "Active"
    assert result.loc[result["email"] == "alpha@example.com", "phone_e164"].iloc[0].startswith("+63")
    assert pd.api.types.is_datetime64_any_dtype(result["birthdate"])
    mock_to_sql.assert_called_once()
    context.add_output_metadata.assert_called_once()


def test_gold_customers_creates_derived_columns_and_filters_invalid_ages():
    context = build_asset_context()
    context.add_output_metadata = MagicMock()

    silver_df = pd.DataFrame(
        {
            "email": ["a@example.com", "b@example.com", "c@example.com"],
            "customer_id": [1, 2, 3],
            "first_name": ["A", "B", "C"],
            "last_name": ["One", "Two", "Three"],
            "birthdate": ["1990-01-01", "2000-01-01", "2100-01-01"],
            "total_relationship_balance": [1000, 5_000_000, 2_000_000],
            "tenure_months": [20, 90, 150],
        }
    )

    database = MagicMock()
    database.get_engine.return_value = MagicMock()

    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        result = gold_customers(context=context, silver_customers=silver_df, database=database)

    assert len(result) == 2
    assert {"value_tier", "tenure", "age", "completeness_score", "gold_timestamp"}.issubset(result.columns)
    assert result["value_tier"].isin(["Standard", "Medium", "High"]).all()
    assert result["tenure"].isin(["New", "Medium Term", "Long Term"]).all()
    assert pd.api.types.is_numeric_dtype(result["completeness_score"])
    mock_to_sql.assert_called_once()
    context.add_output_metadata.assert_called_once()


def test_bronze_customer_checks_fail_for_invalid_email_and_missing_required_fields():
    customer_df = pd.DataFrame(
        {
            "customer_id": ["cust-1"],
            "email": ["invalid-email"],
            "first_name": [None],
        }
    )

    email_result = bronze_customer_email_format(customer_df)
    required_result = bronze_customer_required_fields(customer_df)

    assert email_result.passed is False
    assert required_result.passed is False


def test_check_phone_format_treats_numeric_dot_zero_suffix_as_valid_numbers():
    customer_df = pd.DataFrame({
        "phone_e164": ["+639211686083", "639250824319.0", "639148351224.0", "bad-phone"]
    })

    result = check_phone_format(customer_df)

    assert result.passed is False
    assert result.metadata["invalid_phone_count"] == 2

# Document Ingestion
def test_chromadb_status_healthy():
    # 1. Setup mock Chroma DB
    mock_client = MagicMock()
    mock_client.heartbeat.return_value = None # No error raised
    
    mock_collection = MagicMock()
    mock_collection.count.return_value = 42
    mock_client.get_or_create_collection.return_value = mock_collection
    
    mock_vector_db = MagicMock()
    mock_vector_db.get_client.return_value = mock_client
    
    # 2. Run asset
    context = dg.build_asset_context()
    result = chromadb_status(context=context, vector_db=mock_vector_db)
    
    # 3. Assertions
    assert result["status"] == "Healthy"
    assert result["document_count"] == 42
    assert result["collection_exists"] is True

def test_chromadb_status_unhealthy():
    # Setup mock to raise an exception on heartbeat
    mock_client = MagicMock()
    mock_client.heartbeat.side_effect = Exception("Connection Timeout")
    
    mock_vector_db = MagicMock()
    mock_vector_db.get_client.return_value = mock_client
    
    context = dg.build_asset_context()
    result = chromadb_status(context=context, vector_db=mock_vector_db)
    
    assert result["status"] == "Unhealthy: Connection Timeout"

def test_ingest_aborts_if_upstream_unhealthy():
    context = dg.build_asset_context()
    bad_status = {"status": "Unhealthy: DB Down"}
    
    # Verify the Exception is raised properly
    with pytest.raises(Exception, match="Aborting ingestion: ChromaDB is not healthy"):
        ingest_products(
            context=context, 
            chromadb_status=bad_status, 
            data=MagicMock(), 
            vector_db=MagicMock()
        )

@patch("src.pipelines.defs.document_ingestion.extract_text")
@patch("src.pipelines.defs.document_ingestion.chunk_text")
def test_ingest_products_success(
    mock_chunk_text,
    mock_extract_text,
    mock_context,
    mock_data_resource,
    mock_chroma_resource,
):
    mock_extract_text.return_value = "This is fake PDF content."
    mock_chunk_text.return_value = ["Fake Chunk 1", "Fake Chunk 2"]

    result = ingest_products(
        context=mock_context,
        chromadb_status={"status": "Healthy"},
        data=mock_data_resource,
        vector_db=mock_chroma_resource,
    )

    assert result.metadata["status"] == "success"
    assert result.metadata["processed_count"] == 2
    assert result.metadata["failed_count"] == 0
    assert result.metadata["total_chunks"] == 4

    mock_client = mock_chroma_resource.get_client()
    mock_collection = mock_client.get_or_create_collection()

    assert mock_collection.upsert.call_count == 2

def test_check_ingestion_counts_passes():
    # Setup mock to return metadata for 2 chunks from 1 file
    mock_collection = MagicMock()
    mock_collection.get.return_value = {
        "metadatas": [
            {"source_file": "doc1.pdf"},
            {"source_file": "doc1.pdf"}
        ]
    }
    
    mock_vector_db = MagicMock()
    mock_vector_db.get_client().get_collection.return_value = mock_collection
    
    result = check_ingestion_counts_greater_than_zero(mock_vector_db)
    
    assert result.passed is True
    assert result.metadata["processed_count"].value == 1 # 1 unique file
    assert result.metadata["total_chunks"].value == 2    # 2 chunks total

def test_check_ingestion_counts_fails_on_empty():
    mock_collection = MagicMock()
    # Simulate an empty database
    mock_collection.get.return_value = {"metadatas": []}
    
    mock_vector_db = MagicMock()
    mock_vector_db.get_client().get_collection.return_value = mock_collection
    
    result = check_ingestion_counts_greater_than_zero(mock_vector_db)
    
    assert result.passed is False
    assert "Database is empty" in result.description