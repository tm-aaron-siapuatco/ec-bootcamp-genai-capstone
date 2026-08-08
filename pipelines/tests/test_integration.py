import pandas as pd
import dagster as dg
from src.pipelines.defs.silver_pipeline import silver_customers
from src.pipelines.defs.gold_pipeline import gold_customers

def test_silver_customers_success(test_postgres_resource):
    """Test that pipeline successfully transforms and merges clean data."""
    
    mock_core = pd.DataFrame({
        "customer_id": ["1001", "1002"],
        "email": ["alice@test.com", "bob@test.com"],
        "first_name": ["Alice", "Bob"],
        "middle_name": ["M", "B"], 
        "last_name": ["Smith", "Jones"]
    })
    
    mock_crm = pd.DataFrame({
        "email_address": ["alice@test.com", "bob@test.com"],
        "is_active": ["Y", "N"],
        "opt_in_marketing": ["Y", "N"],
        "total_relationship_balance": ["5,000.50", "10,000.00"],
        "average_daily_balance": ["1,000.00", "2,500.00"],
        "dob": ["1990-01-01", "1985-05-05"],
        "phone_local": ["09123456789", "09987654321"],
        "join_date": ["2023-01-15", "2023-02-20"]
    })

    context = dg.build_asset_context()
    
    result_df = silver_customers(context, mock_core, mock_crm, test_postgres_resource)
    
    # Assertions
    assert len(result_df) == 2
    
    # Check Core transformations
    assert result_df.loc[result_df["email"] == "alice@test.com", "full_name"].iloc[0] == "Alice M Smith"
    
    # Check CRM transformations (status mapping, float conversion, phone regex, renaming)
    alice_row = result_df[result_df["email"] == "alice@test.com"].iloc[0]
    assert alice_row["status"] == "Active"
    assert alice_row["marketing_opt_in"] == True
    assert alice_row["total_relationship_balance"] == 5000.50
    assert alice_row["phone_e164"] == "+639123456789"

def test_silver_customers_drops_invalid_rows(test_postgres_resource):
    """Test that rows missing critical identifiers are dropped before saving."""
    
    mock_core = pd.DataFrame({
        "customer_id": ["1001", None], # Bob is missing a customer_id
        "email": ["alice@test.com", "bob@test.com"],
        "first_name": ["Alice", "Bob"],
        "middle_name": ["M", "B"],
        "last_name": ["Smith", "Jones"]
    })
    
    mock_crm = pd.DataFrame({
        "email_address": ["alice@test.com", "bob@test.com"],
        "is_active": ["Y", "Y"],
        "opt_in_marketing": ["Y", "Y"],
        "total_relationship_balance": ["5,000.50", "10,000.00"],
        "average_daily_balance": ["1,000.00", "2,500.00"],
        "dob": ["1990-01-01", "1985-05-05"],
        "phone_local": ["09123456789", "09987654321"],
        "join_date": ["2023-01-15", "2023-02-20"]
    })

    context = dg.build_asset_context()
    
    result_df = silver_customers(context, mock_core, mock_crm, test_postgres_resource)
    
    # Bob should be dropped because his customer_id is None
    assert len(result_df) == 1
    assert "bob@test.com" not in result_df["email"].values
    
def test_silver_customers_database_integration(test_postgres_resource):
    # 1. Create fake upstream data
    fake_core = pd.DataFrame({
        "customer_id": ["123"], 
        "first_name": ["John"], 
        "middle_name": ["C"],
        "last_name": ["Doe"], 
        "email": ["john@test.com"], 
        "birthdate": ["1990-01-01"]
    })
    fake_crm = pd.DataFrame({
        "email_address": ["john@test.com"],
        "is_active": ["Y"], 
        "opt_in_marketing": ["Y"],
        "total_relationship_balance": ["15,000.00"],
        "average_daily_balance": ["3,500.00"],
        "dob": ["1980-10-10"],
        "phone_local": ["09111111111"],
        "join_date": ["2023-03-01"]
    })

    # 2. Run the asset
    context = dg.build_asset_context()
    result_df = silver_customers(
        context=context,
        retrieve_raw_customers=fake_core,
        retrieve_raw_crm_contacts=fake_crm,
        database=test_postgres_resource
    )

    # 3. INTEGRATION CHECK: Connect to the actual test database and read it back
    engine = test_postgres_resource.get_engine()
    db_results = pd.read_sql_table("silver_customers", con=engine, schema="public")

    # 4. Verify the database wrote and read successfully
    assert len(db_results) == 1
    assert db_results.iloc[0]["total_relationship_balance"] == 15000

def test_gold_customers_business_logic(test_postgres_resource):
    # 1. Create a dynamic "now" to make age calculations robust
    now = pd.Timestamp.now()
    
    # 2. Build mock Silver data with targeted edge cases
    mock_silver = pd.DataFrame({
        "customer_id": ["C1", "C2", "C3", "C4"],
        
        # Test Value Tiers: Standard (<1.5M), Medium (1.5M-3M), High (>3M)
        "total_relationship_balance": [500_000, 2_000_000, 5_000_000, 1_000_000],
        
        # Test Tenure Tiers: New (<42), Medium Term (42-85), Long Term (>85)
        "tenure_months": [10, 60, 100, 10],
        
        # Test Ages: Normal (~30), Missing (NaN), Impossible High (150), Impossible Low (-5)
        "birthdate": [
            (now - pd.DateOffset(years=30)).strftime("%Y-%m-%d"), 
            None, 
            (now - pd.DateOffset(years=150)).strftime("%Y-%m-%d"), # Should be dropped
            (now + pd.DateOffset(years=5)).strftime("%Y-%m-%d")    # Should be dropped
        ]
    })

    # 3. Run the Gold asset
    context = dg.build_asset_context()
    result_df = gold_customers(
        context=context,
        silver_customers=mock_silver,
        database=test_postgres_resource
    )

    # 4. MEMORY ASSERTIONS: Check the returned DataFrame
    
    # We started with 4 rows, but 2 had impossible ages. Only 2 should remain.
    assert len(result_df) == 2, "Failed to drop invalid ages"
    
    # Check Customer 1 (Standard, New, ~30 years old)
    c1 = result_df[result_df["customer_id"] == "C1"].iloc[0]
    assert c1["value_tier"] == "Standard"
    assert c1["tenure"] == "New"
    assert c1["age"] == 30
    
    # Check Customer 2 (Medium, Medium Term, NaN age allowed)
    c2 = result_df[result_df["customer_id"] == "C2"].iloc[0]
    assert c2["value_tier"] == "Medium"
    assert c2["tenure"] == "Medium Term"
    assert pd.isna(c2["age"]), "Failed to preserve NaN ages"

    # 5. DATABASE ASSERTIONS: Verify it wrote to SQLite successfully
    engine = test_postgres_resource.get_engine()
    
    # Read the data back using the "public" schema attached in the fixture
    db_results = pd.read_sql_table("gold_customers", con=engine, schema="public")
    
    assert len(db_results) == 2
    assert "gold_timestamp" in db_results.columns
    assert "completeness_score" in db_results.columns