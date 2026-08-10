from .resources import DataResources, PostgresResource
from ..utils import upsert_dataframe
import dagster as dg
import pandas as pd
import re

class RawCustomersConfig(dg.Config):
    """Optional single-file target; when unset, processes the base core_customers.csv."""
    target_file: str | None = None

class RawCrmContactsConfig(dg.Config):
    """Optional single-file target; when unset, processes the base crm_contacts.csv."""
    target_file: str | None = None

@dg.asset(group_name="medallion")
def retrieve_raw_customers(
    context: dg.AssetExecutionContext,
    config: RawCustomersConfig,
    data: DataResources,
    database: PostgresResource,
) -> pd.DataFrame:
    df = data.read_csv(config.target_file or "core_customers.csv")

    engine = database.get_engine()
    upsert_dataframe(df, table_name="bronze_customers", key_column="customer_id", engine=engine)

    # Logging purposes
    total_rows = len(df)
    null_counts = df.isnull().sum().to_dict()
    total_nulls = sum(null_counts.values())

    context.add_output_metadata({
         "total_rows": total_rows,
         "total_nulls": total_nulls,
         "null_counts_by_col": null_counts
    })

    return df

@dg.asset_check(asset = retrieve_raw_customers, blocking=True)
def non_empty_customers(retrieve_raw_customers: pd.DataFrame):
    total_rows = len(retrieve_raw_customers)
    return dg.AssetCheckResult(
        passed=total_rows > 0,
        metadata = {"Total Rows": total_rows}
    )


@dg.asset_check(asset=retrieve_raw_customers, blocking=True)
def bronze_customer_email_format(retrieve_raw_customers: pd.DataFrame):
    if "email" not in retrieve_raw_customers.columns:
        return dg.AssetCheckResult(
            passed=False,
            description="Missing required 'email' column.",
        )

    EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    email_series = retrieve_raw_customers["email"].dropna().astype(str)
    invalid_emails = email_series[~email_series.str.fullmatch(EMAIL_REGEX.pattern, na=False)].tolist()

    return dg.AssetCheckResult(
        passed=len(invalid_emails) == 0,
        metadata={
            "invalid_email_count": len(invalid_emails),
            "invalid_emails": invalid_emails[:10],
        },
    )


@dg.asset_check(asset=retrieve_raw_customers, blocking=True)
def bronze_customer_required_fields(retrieve_raw_customers: pd.DataFrame):
    required_columns = ["customer_id", "email", "first_name", "last_name"]
    missing_values = {}

    for column in required_columns:
        if column in retrieve_raw_customers.columns:
            missing_values[column] = int(retrieve_raw_customers[column].isna().sum())
        else:
            missing_values[column] = "missing_column"

    # No `isinstance` filter here: a "missing_column" string must fail the
    # `== 0` comparison too, not be silently excluded from the check.
    passed = all(value == 0 for value in missing_values.values())

    return dg.AssetCheckResult(
        passed=passed,
        metadata={"required_field_missing_values": missing_values},
    )


@dg.asset(group_name="medallion")
def retrieve_raw_crm_contacts(
    context: dg.AssetExecutionContext,
    config: RawCrmContactsConfig,
    data: DataResources,
    database: PostgresResource,
) -> pd.DataFrame:
    df = data.read_csv(config.target_file or "crm_contacts.csv")
    engine = database.get_engine()

    # Bronze preserves the source data as-is, including duplicate cust_ids --
    # silver_customers already dedupes by email during its own cleanup pass.
    upsert_dataframe(df, table_name="bronze_contacts", key_column="cust_id", engine=engine)

    # Logging purposes
    total_rows = len(df)
    null_counts = df.isnull().sum().to_dict()
    total_nulls = sum(null_counts.values())

    context.add_output_metadata({
             "total_rows": total_rows,
             "total_nulls": total_nulls,
             "null_counts_by_col": null_counts
        })
    
    return df

@dg.asset_check (asset= retrieve_raw_crm_contacts, blocking=True)
def non_empty_contacts(retrieve_raw_crm_contacts:pd.DataFrame):
    total_rows = len(retrieve_raw_crm_contacts)

    return dg.AssetCheckResult(
        passed= total_rows > 0,
        metadata = {"Total Rows": total_rows}   
    )