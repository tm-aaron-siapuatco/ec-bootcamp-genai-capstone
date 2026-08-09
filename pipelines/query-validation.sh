QUERY_TEXT="Astra Travel Miles Platinum" \
   EXPECTED_SOURCE_FILE="Astra_Travel_Miles_Platinum.pdf" \
   uv run python -c '
   import os
   import chromadb
   from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction
   from dotenv import load_dotenv

   load_dotenv()
   embedding_function = OpenAIEmbeddingFunction(
       api_key=os.environ["AZURE_OPENAI_API_KEY"],
       api_base=os.environ["AZURE_OPENAI_API_ENDPOINT"],
       api_version=os.getenv("AZURE_OPENAI_API_VERSION", "latest"),
       model_name=os.getenv("AZURE_OPENAI_API_EMBEDDING_MODEL", "text-embedding-3-large"),
       deployment_id=os.getenv("AZURE_OPENAI_API_EMBEDDING_MODEL", "text-embedding-3-large"),
       api_type="azure",
   )
   client = chromadb.HttpClient(
       host=os.getenv("CHROMADB_HOST", "localhost"),
       port=int(os.getenv("CHROMADB_PORT", "8000")),
   )
   collection = client.get_collection(
       name=os.getenv("CHROMADB_COLLECTION_NAME", "knowledge_base"),
       embedding_function=embedding_function,
   )

   results = collection.query(
       query_texts=[os.environ["QUERY_TEXT"]],
       n_results=3,
       include=["documents", "metadatas", "distances"],
   )
   documents = results["documents"][0]
   metadatas = results["metadatas"][0]
   assert documents, "The query returned no documents"
   assert any(
       metadata.get("source_file") == os.environ["EXPECTED_SOURCE_FILE"]
       for metadata in metadatas
   ), "The query did not retrieve the expected product document"

   for document, metadata, distance in zip(
       documents, metadatas, results["distances"][0]
   ):
       assert document.strip(), "A retrieved document chunk is empty"
       assert {"source_file", "chunk_index", "total_chunks"}.issubset(metadata)
       print(
           f"{metadata["source_file"]} chunk {metadata["chunk_index"]} "
           f"distance={distance}"
       )
       print(document[:200].replace("\\n", " "))
   '
