import dagster as dg
import pandas as pd
from .resources import PostgresResource
from ..utils import normalize_phone

@dg.asset(group_name="medallion")
def silver_customers(
    context: dg.AssetExecutionContext,
    retrieve_raw_customers: pd.DataFrame, 
    retrieve_raw_crm_contacts: pd.DataFrame, 
    database: PostgresResource
) -> pd.DataFrame:
    
    # capture counts before any transformations or drops
    core_initial_count = len(retrieve_raw_customers)
    crm_initial_count = len(retrieve_raw_crm_contacts)
    
    customer_df = retrieve_raw_customers.copy()
    crm_df = retrieve_raw_crm_contacts.copy()

    # Transform Core data
    name_columns = ["first_name", "middle_name", "last_name"]
    customer_df["full_name"] = customer_df[name_columns].apply(
        lambda row: " ".join(row.dropna().astype(str).str.strip()), axis=1
    )
    customer_df["email"] = customer_df["email"].astype(str).str.lower().str.strip()

    # Transform CRM data
    crm_df["status"] = crm_df["is_active"].map({"Y": "Active", "N": "Dormant"})
    crm_df["opt_in_marketing"] = crm_df["opt_in_marketing"].map({"Y": True, "N": False})
    
    crm_df["total_relationship_balance"] = crm_df["total_relationship_balance"].astype(str).str.replace(",", "").astype(float)
    crm_df["average_daily_balance"] = crm_df["average_daily_balance"].astype(str).str.replace(",", "").astype(float)

    renaming_keys = {
        "dob": "birthdate",
        "phone_local": "phone_e164", 
        "region": "province",
        "zip_code": "postal_code",
        "country_code": "country",
        "join_date": "created_at",
        "kyc_level_text": "kyc_level",
        "tier": "hexagon_tier",
        "segment_name": "segment",
        "opt_in_marketing": "marketing_opt_in"
    }
    
    # Handle email consolidation
    if "email" in crm_df.columns and "email_address" in crm_df.columns:
        crm_df["email"] = crm_df["email"].where(
            crm_df["email"].notna() & crm_df["email"].astype(str).str.strip().ne(""),
            crm_df["email_address"],
        )
        crm_df = crm_df.drop(columns=["email_address"], errors="ignore")
    elif "email_address" in crm_df.columns:
        crm_df = crm_df.rename(columns={"email_address": "email"})
        
    if "phone_e164" in customer_df.columns:
        customer_df["phone_e164"] = customer_df["phone_e164"].apply(normalize_phone)
    
    # Apply renames and final string cleaning
    crm_df = crm_df.rename(columns=renaming_keys, errors="ignore")
    crm_df['phone_e164'] = crm_df['phone_e164'].apply(normalize_phone)
    crm_df["email"] = crm_df["email"].astype(str).str.lower().str.strip()
    crm_df = crm_df.drop(columns=["is_active"], errors="ignore")

    # Drop internal duplicates before merging
    customer_df = customer_df.drop_duplicates(subset=["email"], keep="first")
    crm_df = crm_df.drop_duplicates(subset=["email"], keep="first")
    
    # Merge datasets using email as index, prioritizing Core data
    merged_df = customer_df.set_index("email").combine_first(crm_df.set_index("email")).reset_index()

    # Drop rows missing critical identifiers and standardize dates
    critical_columns = ["email", "customer_id", "first_name", "last_name", "birthdate", "total_relationship_balance"]
    merged_df = merged_df.dropna(subset=critical_columns)
    
    merged_df["birthdate"] = pd.to_datetime(merged_df["birthdate"], errors="coerce")
    merged_df["created_at"] = pd.to_datetime(merged_df["created_at"], errors="coerce")
    merged_df["silver_timestamp"] = pd.Timestamp.now()
    merged_df["phone_e164"] = merged_df["phone_e164"].astype("string")
    # Logging counts
    post_dedup_and_dropna_count = len(merged_df)
    total_records_ingested = core_initial_count + crm_initial_count
    total_dropped_rows = total_records_ingested - post_dedup_and_dropna_count
    
    null_counts = merged_df.isnull().sum().to_dict()
    total_nulls = sum(null_counts.values())

    # Log metadata and write to database
    context.add_output_metadata({
        "core_records_ingested": core_initial_count,
        "crm_records_ingested": crm_initial_count,
        "total_dropped_rows (duplicates + nulls)": total_dropped_rows,
        "final_silver_rows": post_dedup_and_dropna_count,
        "total_nulls": total_nulls,
        "null_counts_by_col": null_counts,
        "phone_e164_non_null_count": int(merged_df["phone_e164"].notna().sum()),
    })

    engine = database.get_engine()
    schema_name = "public" if engine.dialect.name == "postgresql" else None
    merged_df.to_sql(
        name="silver_customers",
        con=engine,
        schema="public",
        if_exists="replace",
        index=False
    )
    
    return merged_df

@dg.asset_check(asset="silver_customers", blocking=True)
def unique_customer_ids(silver_customers: pd.DataFrame) -> dg.AssetCheckResult:
    duplicate_count = silver_customers["customer_id"].duplicated().sum()

    return dg.AssetCheckResult(
        passed = bool(duplicate_count == 0),
        metadata = {"duplicate_customer_ids": int(duplicate_count)}
    )

@dg.asset_check(
    asset="silver_customers",
    description="Ensures all phone numbers match the +63 E.164 format",
    blocking=True,
)
def check_phone_format(silver_customers: pd.DataFrame) -> dg.AssetCheckResult:
    # Dropna because some users might not have a phone number, which is fine
    phones = silver_customers["phone_e164"].dropna().astype(str)
    
    # Check if they match +63 followed by exactly 10 digits
    invalid_phones = phones[~phones.str.match(r'^\+63\d{10}$')]
    
    return dg.AssetCheckResult(
        passed=bool(len(invalid_phones) == 0),
        metadata={
            "invalid_phone_count": len(invalid_phones),
            "sample_invalid_phones": invalid_phones.head(5).tolist() # Logs a sample to help debugging
        }
    )

@dg.asset_check(
    asset="silver_customers",
    description="Ensures birthdates are not in the future and users are reasonably aged",
    blocking=True,
)
def check_logical_birthdates(silver_customers: pd.DataFrame) -> dg.AssetCheckResult:
    birthdates = silver_customers["birthdate"].dropna()
    
    # Check for future birthdates or unrealistically old dates (e.g., before 1900)
    now = pd.Timestamp.now()
    future_dates = (birthdates > now).sum()
    impossible_old_dates = (birthdates.dt.year < 1900).sum()
    
    total_violations = future_dates + impossible_old_dates
    
    return dg.AssetCheckResult(
        passed=bool(total_violations == 0),
        metadata={
            "future_birthdates": int(future_dates),
            "pre_1900_birthdates": int(impossible_old_dates)
        }
    )