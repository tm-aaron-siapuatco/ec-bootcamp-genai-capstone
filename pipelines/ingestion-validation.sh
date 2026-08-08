uv run python -c '
   import os
   import chromadb
   from dotenv import load_dotenv

   load_dotenv()
   client = chromadb.HttpClient(
       host=os.getenv("CHROMADB_HOST", "localhost"),
       port=int(os.getenv("CHROMADB_PORT", "8000")),
   )
   collection = client.get_collection(
       os.getenv("CHROMADB_COLLECTION_NAME", "knowledge_base")
   )
   metadatas = collection.get(include=["metadatas"])["metadatas"]
   assert metadatas, "The collection contains no metadata records"

   required = {"source_file", "chunk_index", "total_chunks"}
   for metadata in metadatas:
       assert required.issubset(metadata), f"Missing metadata fields: {required - metadata.keys()}"
       assert isinstance(metadata["source_file"], str) and metadata["source_file"]
       assert isinstance(metadata["chunk_index"], int) and metadata["chunk_index"] >= 0
       assert isinstance(metadata["total_chunks"], int) and metadata["total_chunks"] > 0

   print(f"Verified required metadata on {len(metadatas)} chunks")
'
