import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd

# Ensure env var is set
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# Config
cfg = Config()

# Query function
def sql_query_with_user_token(query: str, user_token: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        access_token=user_token
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# Page setup
st.set_page_config(layout="wide", page_title="Agent Review Browser")

# Global CSS for larger text
st.markdown("""
    <style>
        html, body, .stApp {
            font-size: 32px;
        }
        .quote-link {
            font-size: 32px;
            text-decoration: none;
            display: block;
            margin: 16px 0;
            color: #1a73e8;
        }
        .quote-link:hover {
            text-decoration: underline;
        }
        .stButton button {
            font-size: 28px !important;
        }
        .stTextInput input {
            font-size: 28px !important;
        }
    </style>
""", unsafe_allow_html=True)

# Get token and query param
user_token = st.context.headers.get("X-Forwarded-Access-Token")
params = st.query_params
selected_quote_id = params.get("quote_id")

# SQL to fetch data
query = "SELECT * FROM lrcatalog.agentic_underwriting.agent_review"

try:
    df = sql_query_with_user_token(query, user_token)

    if selected_quote_id:
        row = df[df['quote_id'] == selected_quote_id]
        if not row.empty:
            st.markdown(f"## 🧾 Quote Details: `{selected_quote_id}`")
            quote_data = row.iloc[0].to_dict()
            for key, value in quote_data.items():
                st.markdown(f"**{key}**: {value}")
            if st.button("🔙 Back to all quotes"):
                st.query_params.clear()
        else:
            st.error(f"No data found for quote ID: {selected_quote_id}")
    else:
        st.markdown("## 🔍 Select a Quote to View Details")
        for quote_id in df["quote_id"].unique():
            st.markdown(
                f'<a class="quote-link" href="?quote_id={quote_id}" target="_self">🔗 {quote_id}</a>',
                unsafe_allow_html=True,
            )

except Exception as e:
    st.error(f"Error: {e}")