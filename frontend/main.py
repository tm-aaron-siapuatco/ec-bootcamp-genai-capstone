import streamlit as st
import requests

API_URL = "http://localhost:8001" # Since docker container of chromaDB is 8000


def init_session_state():
    """Set up persistent state that survives Streamlit's rerun-on-every-interaction model."""
    if "messages" not in st.session_state:
        st.session_state.messages = []  # list of {"role": "user"/"assistant", "content": str}
    if "use_postgres" not in st.session_state:
        st.session_state.use_postgres = False
    if "use_chroma" not in st.session_state:
        st.session_state.use_chroma = False
    if "processed_uploads" not in st.session_state:
        st.session_state.processed_uploads = {}  # {filename: (success, message)}


def get_source_string() -> str | None:
    """Translate the two toggles into the source string the backend expects."""
    if st.session_state.use_postgres and st.session_state.use_chroma:
        return "both"
    elif st.session_state.use_chroma:
        return "chroma"
    elif st.session_state.use_postgres:
        return "postgres"
    return None


def call_upload(uploaded_file) -> tuple[bool, str]:
    """Send the uploaded file to the backend and wait for Dagster to finish processing it."""
    try:
        files = {"file": (uploaded_file.name, uploaded_file.getvalue(), uploaded_file.type)}
        response = requests.post(f"{API_URL}/upload", files=files, timeout=180)
        response.raise_for_status()
        data = response.json()
        return data.get("status") == "success", data.get("message", "No message returned.")
    except requests.exceptions.ConnectionError:
        return False, "Couldn't reach the backend. Is the FastAPI server running?"
    except requests.exceptions.Timeout:
        return False, "The backend took too long to process the upload."
    except requests.exceptions.HTTPError as e:
        return False, f"Backend returned an error: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"


def call_backend(prompt: str, source: str, history: list[dict]) -> str:
    """Send the user's message and recent chat history to the FastAPI backend."""
    try:
        response = requests.post(
            f"{API_URL}/chat",
            json={"query": prompt, "data_source": source, "history": history},
            timeout=60,
        )
        response.raise_for_status()
        return response.json().get("answer", "No answer returned.")
    except requests.exceptions.ConnectionError:
        return "Couldn't reach the backend. Is the FastAPI server running?"
    except requests.exceptions.Timeout:
        return "The backend took too long to respond."
    except requests.exceptions.HTTPError as e:
        return f"Backend returned an error: {e}"
    except Exception as e:
        return f"Unexpected error: {e}"


def main():
    st.set_page_config(page_title="RAG Chatbot", layout="wide")
    init_session_state()

    st.title("Gen AI Capstone")
    st.caption("Ask a question, choose a data source, and optionally attach a document.")

    # --- Source toggle buttons ---
    st.markdown("**Data sources**")
    col1, col2 = st.columns(2)

    col1, col2 = st.columns(2)
    with col1:
        st.session_state.use_postgres = st.toggle(
            "Customer Data (Postgres)", value=st.session_state.use_postgres
        )
    with col2:
        st.session_state.use_chroma = st.toggle(
            "Bank Offers (ChromaDB)", value=st.session_state.use_chroma
    )

    source = get_source_string()
    if source is None:
        st.warning("Select at least one data source above to enable chat.")

    # --- File upload ---
    with st.expander("Attach a document (optional)"):
        st.caption(
            "PDF: any bank product doc. CSV: must be named exactly `core_customers.csv` "
            "and/or `crm_contacts.csv` — attach both for a complete customer record."
        )
        uploaded_files = st.file_uploader(
            "Upload PDFs or CSVs to add to the knowledge base",
            type=["pdf", "csv"],
            accept_multiple_files=True,
            label_visibility="collapsed",
        )
        for uploaded in uploaded_files:
            if uploaded.name not in st.session_state.processed_uploads:
                with st.spinner(f"Processing {uploaded.name}… running the Dagster pipeline, this can take a bit."):
                    success, message = call_upload(uploaded)
                st.session_state.processed_uploads[uploaded.name] = (success, message)

        for uploaded in uploaded_files:
            success, message = st.session_state.processed_uploads[uploaded.name]
            if success:
                st.success(f"**{uploaded.name}**: {message}")
            else:
                st.error(f"**{uploaded.name}**: {message}")

    st.divider()

    # --- Chat history ---
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # --- Chat input ---
    prompt = st.chat_input(
        "Type your question..." if source else "Select a data source to start chatting",
        disabled=not source,
    )

    if prompt:
        history = list(st.session_state.messages)  # snapshot before this turn is appended
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                answer = call_backend(prompt, source, history)
            st.markdown(answer)

        st.session_state.messages.append({"role": "assistant", "content": answer})


if __name__ == "__main__":
    main()