import dagster as dg
from .resources import DataResources, ChromaDBResource
from ..utils import extract_text, chunk_text
import os

class IngestProductsConfig(dg.Config):
    """Optional single-file target; when unset, processes the whole PDF directory."""
    target_file: str | None = None

@dg.asset(group_name = "ingest")
def chromadb_status(context: dg.AssetExecutionContext, vector_db: ChromaDBResource):
    """report on health of serice and state of target collection"""
    chroma_client = vector_db.get_client()

    openai_ef = vector_db.embedding_function()
    collection_name = os.getenv("CHROMADB_COLLECTION_NAME", "knowledge_base")

    collection = chroma_client.get_or_create_collection(
            name=collection_name,
            embedding_function=openai_ef
        )
    
    # ping database to ensure connectivity and check if healthy
    try:
        chroma_client.heartbeat()
        status = "Healthy"
    except Exception as e:
        status = f"Unhealthy: {str(e)}"

    doc_count = collection.count()

    result = {
        "status": status,
        "collection_exists": True, 
        "document_count": doc_count,
    }

    context.add_output_metadata(result)
    
    return result

@dg.asset(group_name="ingest")
def ingest_products(
    context: dg.AssetExecutionContext,
    config: IngestProductsConfig,
    chromadb_status: dict,
    data: DataResources,
    vector_db: ChromaDBResource,
):
    """Ingests products pdfs that goes through chunking, embedding and storing in ChromaDB"""

    # Safeguard: Abort if the upstream status is unhealthy
    if not chromadb_status["status"].startswith("Healthy"):
        raise Exception(f"Aborting ingestion: ChromaDB is not healthy. Status: {chromadb_status['status']}")

    # Setup
    chroma_client = vector_db.get_client()
    openai_ef = vector_db.embedding_function()

    # Get from .env collection name
    collection_name = os.getenv("CHROMADB_COLLECTION_NAME", "knowledge_base")

    collection = chroma_client.get_or_create_collection(
        name=collection_name,
        embedding_function=openai_ef
    )

    if config.target_file:
        pdf_files = [data.get_pdf_file_path(config.target_file)]
    else:
        pdf_files = data.get_pdf_files()
    context.log.info(f"Discovered {len(pdf_files)} PDFs for processing.")

    total_chunks = 0
    processed_count = 0
    failed_files = []
    for file_path in pdf_files:
        file_name = os.path.basename(file_path)
        
        try:
            product_name = os.path.splitext(file_name)[0]
            
            # pass full file_path
            text = extract_text(file_path)
            chunks = chunk_text(text)
            num_chunks = len(chunks)

            documents, chunk_ids, metadatas = [], [], []

            for i, chunk in enumerate(chunks):
                # upserts in place instead of piling up duplicate chunks.
                chunk_id = f"{product_name}_{i}"

                documents.append(chunk)
                chunk_ids.append(chunk_id)

                file_extension = os.path.splitext(file_name)[1].lower().lstrip(".")

                metadatas.append({
                    "source_file": file_name,
                    "chunk_index": i,
                    "total_chunks": num_chunks,
                    "file_type": file_extension,
                })

            # Upsert to not duplicate chunks
            collection.delete(where={"source_file": file_name})
            collection.upsert(
                ids=chunk_ids,
                documents=documents,
                metadatas=metadatas,
            )
            
            context.log.info(f"Successfully processed {file_name}: stored {num_chunks} chunks.")
            processed_count += 1
            total_chunks += num_chunks

        except Exception as e:
            context.log.error(f"Failed to process {file_name}: {str(e)}")
            failed_files.append(file_name)
            continue

    if failed_files:
        context.log.warning(f"{len(failed_files)} files failed to process: {failed_files}")

    context.log.info(f"Ingestion complete. Processed {processed_count} files and stored {total_chunks} total chunks.")
    
    return dg.MaterializeResult(
        metadata={
            "status": "success" if processed_count > 0 else "failed",
            "processed_count": processed_count,
            "failed_count": len(failed_files),
            "failed_files": failed_files,
            "total_chunks": total_chunks
        }
    )

@dg.asset_check(asset="ingest_products", description="Ensures processed_count and total_chunks are greater than zero", blocking=True)
def check_ingestion_counts_greater_than_zero(vector_db: ChromaDBResource):
    client = vector_db.get_client()
    collection_name = os.getenv("CHROMADB_COLLECTION_NAME", "knowledge_base")
    collection = client.get_collection(collection_name)
    
    # Pull metadata to evaluate counts
    metadatas = collection.get(include=["metadatas"])["metadatas"]
    
    if not metadatas:
        return dg.AssetCheckResult(
            passed=False, 
            description="Database is empty. Total chunks is 0."
        )
        
    # Calculate total chunks and unique files directly from the database
    total_chunks = len(metadatas)
    unique_files = len(set(meta.get("source_file") for meta in metadatas if meta.get("source_file")))
    
    is_valid = (total_chunks > 0) and (unique_files > 0)
    
    return dg.AssetCheckResult(
        passed=is_valid,
        description=f"Found {unique_files} files and {total_chunks} chunks." if is_valid else "Counts are not greater than zero.",
        metadata={
            "processed_count": unique_files,
            "total_chunks": total_chunks
        }
    )