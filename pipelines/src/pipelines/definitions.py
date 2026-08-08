import dagster as dg
from .defs import bronze_pipeline, silver_pipeline, gold_pipeline, document_ingestion, resources

all_assets = dg.load_assets_from_modules([bronze_pipeline, silver_pipeline, gold_pipeline, document_ingestion])
all_checks = dg.load_asset_checks_from_modules([bronze_pipeline, silver_pipeline, gold_pipeline])
defs = dg.Definitions(
    assets = all_assets,
    asset_checks = all_checks,
    resources = {
        "database": resources.PostgresResource(),
        "data": resources.DataResources(),
        "vector_db": resources.ChromaDBResource()
    }
)
