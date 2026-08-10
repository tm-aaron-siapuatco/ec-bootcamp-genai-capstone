import os
import re
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

TABLE_NAME = "gold_customers"
TOP_K = 3

# Exclude PII national_id / customer_id
DISPLAY_COLUMNS = [
    "full_name", "cust_id", "email", "phone_e164", "city", "province",
    "segment", "hexagon_tier", "kyc_level", "status", "value_tier", "tenure",
    "age", "total_relationship_balance", "average_daily_balance", "marketing_opt_in",
]

EMAIL_REGEX = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
CUST_ID_REGEX = re.compile(r"\bCUST-\d+\b", re.IGNORECASE)
WORD_REGEX = re.compile(r"[A-Za-z]+")

# Filtered out when scanning for name
QUESTION_STOPWORDS = {
    "what", "who", "how", "where", "when", "is", "are", "does", "do", "can",
    "show", "tell", "which", "why", "please", "the", "a", "an", "me", "about",
    "for", "of", "in", "on", "at", "to", "and", "or", "with", "customer",
    "customers", "give", "details", "info", "information", "balance",
}


def get_engine():
    user = os.getenv("DATABASE_USER", "postgres")
    password = os.getenv("DATABASE_PASSWORD", "password")
    host = os.getenv("DATABASE_HOST", "localhost")
    port = os.getenv("DATABASE_PORT", "5432")
    name = os.getenv("DATABASE_NAME", "postgres")
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{name}")


def candidate_name_pairs(question: str) -> list[tuple[str, str]]:
    """Consecutive word-pairs from the question, stopwords removed, case-insensitive."""
    words = [w for w in WORD_REGEX.findall(question) if w.lower() not in QUESTION_STOPWORDS]
    return [(words[i], words[i + 1]) for i in range(len(words) - 1)]


def row_to_text(row: pd.Series) -> str:
    lines = [f"{col}: {row[col]}" for col in DISPLAY_COLUMNS if col in row and pd.notna(row[col])]
    return "\n".join(lines)


def run_query(sql: str, params: dict, top_k: int) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params={**params, "limit": top_k})


def to_result(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    documents = [row_to_text(row) for _, row in df.iterrows()]
    # cust_id is only present for customers with a matched CRM row -- a
    sources = [
        f"gold_customers ({row['cust_id'] if pd.notna(row['cust_id']) else row['customer_id']})"
        for _, row in df.iterrows()
    ]
    return documents, sources


def retrieve(question: str, top_k: int = TOP_K) -> tuple[list[str], list[str]]:
    """Look up matching gold_customers rows by email, customer ID, or name in the question."""
    email_match = EMAIL_REGEX.search(question)
    if email_match:
        df = run_query(
            f"SELECT * FROM {TABLE_NAME} WHERE email ILIKE :value LIMIT :limit",
            {"value": f"%{email_match.group(0).lower()}%"},
            top_k,
        )
        if not df.empty:
            return to_result(df)

    cust_id_match = CUST_ID_REGEX.search(question)
    if cust_id_match:
        df = run_query(
            f"SELECT * FROM {TABLE_NAME} WHERE cust_id = :value LIMIT :limit",
            {"value": cust_id_match.group(0).upper()},
            top_k,
        )
        if not df.empty:
            return to_result(df)

    for first, last in candidate_name_pairs(question):
        df = run_query(
            f"SELECT * FROM {TABLE_NAME} WHERE first_name ILIKE :first AND last_name ILIKE :last LIMIT :limit",
            {"first": f"%{first}%", "last": f"%{last}%"},
            top_k,
        )
        if not df.empty:
            return to_result(df)

    return [], []