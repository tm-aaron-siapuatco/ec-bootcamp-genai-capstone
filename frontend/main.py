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
    if "uploaded_file" not in st.session_state:
        st.session_state.uploaded_file = None


def get_source_string() -> str | None:
    """Translate the two toggles into the source string the backend expects."""
    if st.session_state.use_postgres and st.session_state.use_chroma:
        return "both"
    elif st.session_state.use_chroma:
        return "chroma"
    elif st.session_state.use_postgres:
        return "postgres"
    return None


def call_backend(prompt: str, source: str, uploaded_file) -> str:
    """Send the user's message (and optional file) to the FastAPI backend."""
    try:
        files = None
        if uploaded_file is not None:
            files = {"file": (uploaded_file.name, uploaded_file.getvalue(), uploaded_file.type)}

        response = requests.post(
            f"{API_URL}/chat",
            data={"query": prompt, "source": source},
            files=files,
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
        uploaded = st.file_uploader(
            "Upload a PDF or text file to include as context",
            type=["pdf", "txt", "csv"],
            label_visibility="collapsed",
        )
        if uploaded is not None:
            st.session_state.uploaded_file = uploaded
            st.success(f"Attached: {uploaded.name}")

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
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                answer = call_backend(prompt, source, st.session_state.uploaded_file)
            st.markdown(answer)

        st.session_state.messages.append({"role": "assistant", "content": answer})


if __name__ == "__main__":
    main()