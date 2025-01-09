import streamlit as st
from snowflake.snowpark import Session
from snowflake.cortex import Complete
from snowflake.core import Root
import pandas as pd
import json

pd.set_option("max_colwidth", None)

# Snowflake connection parameters using Streamlit secrets
connection_parameters = {
    "account": st.secrets["SNOWFLAKE_ACCOUNT"],
    "user": st.secrets["SNOWFLAKE_USER"],
    "password": st.secrets["SNOWFLAKE_PASSWORD"],
    "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
    "database": st.secrets["SNOWFLAKE_DATABASE"],
    "schema": st.secrets["SNOWFLAKE_SCHEMA"],
}

try:
    session = Session.builder.configs(connection_parameters).create()
    print("Connection successful!")
except Exception as e:
    print(f"Error: {e}")


# Default values
NUM_CHUNKS = 5
CORTEX_SEARCH_DATABASE = "BASIC_KNOWLEDGE_GRAPH"
CORTEX_SEARCH_SCHEMA = "DATA"
CORTEX_SEARCH_SERVICE = "CC_SEARCH_SERVICE_CS"
COLUMNS = ["chunk", "relative_path", "category"]

# Initialize Snowflake Cortex Services
root = Root(session)
svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

# Functions for the Streamlit app
def config_options():
    st.sidebar.selectbox('Select your model:', ('mistral-large',), key="model_name")

    categories = session.table('docs_chunks_table').select('category').distinct().collect()
    cat_list = ['ALL'] + [cat.CATEGORY for cat in categories]

    st.sidebar.selectbox('Select product category:', cat_list, key="category_value")
    st.sidebar.checkbox('Remember chat history?', key="use_chat_history", value=True)
    st.sidebar.checkbox('Debug mode?', key="debug", value=False)
    st.sidebar.button("Start Over", key="clear_conversation", on_click=init_messages)
    st.sidebar.expander("Session State").write(st.session_state)

def init_messages():
    if st.session_state.get("clear_conversation", False) or "messages" not in st.session_state:
        st.session_state.messages = []

def get_similar_chunks_search_service(query):
    if st.session_state.category_value == "ALL":
        response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
    else:
        filter_obj = {"@eq": {"category": st.session_state.category_value}}
        response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)
    return response.json()

def answer_question(question):
    response = get_similar_chunks_search_service(question)
    return json.dumps(response, indent=2), None

def main():
    st.title("Knowledge Graph Document Assistant using Snowflake Cortex")

    # List available documents
    try:
        docs_available = session.sql("ls @docs").collect()
        list_docs = [doc["name"] for doc in docs_available]
        st.dataframe(list_docs)
    except Exception as e:
        st.error(f"Error fetching documents: {e}")

    # Configure sidebar options
    config_options()
    init_messages()

    # Display chat messages from history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Accept user input
    if question := st.chat_input("What do you want to know about your products?"):
        st.session_state.messages.append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.markdown(question)

        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            question = question.replace("'", "")

            with st.spinner(f"{st.session_state.model_name} thinking..."):
                try:
                    response, relative_paths = answer_question(question)
                    message_placeholder.markdown(response)
                except Exception as e:
                    st.error(f"Error processing query: {e}")

        st.session_state.messages.append({"role": "assistant", "content": response})

if __name__ == "__main__":
    main()