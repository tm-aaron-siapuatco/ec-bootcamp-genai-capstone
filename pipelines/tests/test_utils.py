from pipelines.utils import chunk_text

def test_chunk_text():
    sample_text = "A" * 2000 # Simulate a long document
    chunks = chunk_text(sample_text)
    
    assert len(chunks) > 0
    assert len(chunks[0]) <= 1000 # Assuming a 1000-character chunk limit