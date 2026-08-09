from .resources import PostgresResource
import dagster as dg
import pandas as pd

@dg.asset(group_name="medallion")
def gold_customers(context: dg.AssetExecutionContext, silver_customers: pd.DataFrame, database: PostgresResource) -> pd.DataFrame:
    df = silver_customers.copy()

    # Calculate derived columns

    # Value Tier
    value_bins = [0, 1_500_000, 3_000_000, float("inf")]
    value_tiers = ["Standard", "Medium", "High"]
    df["value_tier"] = pd.cut(df["total_relationship_balance"], bins=value_bins, labels=value_tiers)

    # Calculate tenure
    tenure_bins = [0,42,85,float("inf")]
    tenure_tiers = ["New", "Medium Term", "Long Term"]
    df["tenure"] = pd.cut(df["tenure_months"], bins = tenure_bins, labels=tenure_tiers)

    # Age calculation based on date of birth
    df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce")
    df["age"] = (pd.Timestamp.now() - df["birthdate"]).dt.days // 365
    
    # Data Completeness -> percentage of completeness 
    df["completeness_score"] = (df.notna().mean(axis=1) * 100).round(2)

    # Filter out impossible ages (e.g., negative or over 120) but keep NaNs safely
    df = df[((df["age"] >= 0) & (df["age"] <= 120)) | df["age"].isna()]
    
    # Add timestamp for observability logs
    df["gold_timestamp"] = pd.Timestamp.now()

    # Write to DB
    engine = database.get_engine()
    df.to_sql(
        name="gold_customers",
        con=engine,
        schema="public",
        if_exists="replace",
        index=False
    )

    # Calculate metadata metrics before writing to SQL
    initial_rows = len(silver_customers)
    final_rows = len(df)
    invalid_ages_dropped = initial_rows - final_rows
    
    avg_completeness = float(df["completeness_score"].mean())
    
    # Convert category counts to dicts for JSON logging
    value_tier_dist = df["value_tier"].value_counts().to_dict()
    tenure_dist = df["tenure"].value_counts().to_dict()

    null_counts = df.isnull().sum().to_dict()
    total_nulls = sum(null_counts.values())
    
    context.add_output_metadata({
        "total_gold_records": final_rows,
        "invalid_age_records_dropped": invalid_ages_dropped,
        "average_completeness_score": avg_completeness,
        "value_tier_distribution": dg.MetadataValue.json(value_tier_dist),
        "tenure_distribution": dg.MetadataValue.json(tenure_dist),
        "total_nulls": total_nulls,
        "null_counts_by_col": null_counts
    })
    
    return df

@dg.asset_check(asset=gold_customers, description="Ensures all customers were successfully bucketed into tiers", blocking=True)
def check_no_unassigned_tiers(gold_customers: pd.DataFrame) -> dg.AssetCheckResult:
    # Check if pd.cut produced any NaNs due to out-of-bound values (like negative balances)
    unassigned_value = int(gold_customers["value_tier"].isna().sum())
    unassigned_tenure = int(gold_customers["tenure"].isna().sum())
    
    total_unassigned = unassigned_value + unassigned_tenure
    
    return dg.AssetCheckResult(
        passed=bool(total_unassigned == 0),
        metadata={
            "customers_missing_value_tier": unassigned_value,
            "customers_missing_tenure_tier": unassigned_tenure,
        }
    )