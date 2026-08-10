from pypdf import PdfReader
import pandas as pd
import re
from sqlalchemy import text

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

def upsert_dataframe(df: pd.DataFrame, table_name: str, key_column: str, engine) -> None:
    """ 
    Upsert a DataFrame into a Postgres table by key_column instead of replacing it.
    Deletes any existing rows matching this DataFrame's keys, then appends --
    """
    if key_column not in df.columns:
        raise ValueError(f"upsert_dataframe: key column '{key_column}' not present in DataFrame")

    with engine.begin() as conn:
        table_exists = conn.execute(
            text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = :t)"),
            {"t": table_name},
        ).scalar()

        if table_exists:
            conn.execute(
                text(f'DELETE FROM "{table_name}" WHERE "{key_column}" = ANY(:keys)'),
                {"keys": df[key_column].tolist()},
            )

        df.to_sql(name=table_name, con=conn, schema="public", if_exists="append", index=False)