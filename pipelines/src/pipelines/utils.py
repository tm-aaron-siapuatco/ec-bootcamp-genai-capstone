from pypdf import PdfReader
import pandas as pd
import re

def normalize_phone(value):
    if pd.isna(value):
        return pd.NA
    s = str(value).strip()
    if s.endswith(".0"):          # undo float stringification
        s = s[:-2]
    s = re.sub(r"\D", "", s)      # strip any non-digit chars
    if s.startswith("0"):
        s = "63" + s[1:]          # local format -> country code, no leading 0
    elif not s.startswith("63"):
        s = "63" + s              # bare subscriber number, no country code at all
    return "+" + s

def extract_text(file) -> str:
    text = ""
    reader = PdfReader(file)
    for page in reader.pages:
        text += page.extract_text() or ""
    return text

#TODO: Once we can evaluate the LLM's reponse, adjust accordingly the chunking values or methods
def chunk_text(text:str, chunk_size: int = 1000, overlap: int = 200):
    if len(text) <= chunk_size:
        return [text]

    start, chunks = 0, []

    while start < len(text):
        end = start + chunk_size
        if end > len(text):
            end = len(text)

        chunk = text[start:end]
        chunks.append(chunk)

        if end == len(text): 
            break

        start += chunk_size - overlap

    return chunks
