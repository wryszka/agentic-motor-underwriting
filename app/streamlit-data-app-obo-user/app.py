import os
import time
import streamlit as st
import pandas as pd
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# --- OAuth-based Config ---
cfg = Config()
w = WorkspaceClient()

# --- SQL Query Helper ---
def query_sql(query: str, user_token: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        access_token=user_token
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall_arrow().to_pandas()

# --- Streamlit Page Setup ---
st.set_page_config(layout="wide", page_title="Agent Review App")

st.markdown("""
    <style>
        html, body, .stApp {
            font-size: 24px;
        }

        .quote-link {
            font-size: 24px;
            text-decoration: none;
            display: block;
            margin: 12px 0;
            color: #1a73e8;
        }

        .quote-link:hover {
            text-decoration: underline;
        }

        .stButton button, .stTextInput input, .stTextArea textarea {
            font-size: 24px !important;
        }

        .block-container {
            max-width: 100% !important;
            padding: 2rem 4rem;
        }

        label {
            font-size: 24px !important;
        }
    </style>
""", unsafe_allow_html=True)

# --- Auth & Query Params ---
user_token = st.context.headers.get("X-Forwarded-Access-Token")
params = st.query_params
selected_quote_id = params.get("quote_id")

# --- Load agent review table ---
try:
    df = query_sql("SELECT * FROM lrcatalog.agentic_underwriting.agent_review", user_token)

    if selected_quote_id:
        # ----------------------------
        # Detail Page for Selected Quote
        # ----------------------------
        row = df[df['quote_id'] == selected_quote_id]
        if not row.empty:
            st.markdown(f"## 🧾 Agent Review: `{selected_quote_id}`")
            for k, v in row.iloc[0].to_dict().items():
                st.markdown(f"**{k}**: {v}")
        else:
            st.error(f"No agent review found for quote ID: {selected_quote_id}")

        # --- Editable Quote Section ---
        st.markdown("### ✏️ Edit Matching Quote Input")
        quote_df = query_sql(
            f"SELECT * FROM lrcatalog.agentic_underwriting.quotes WHERE quote_id = '{selected_quote_id}'",
            user_token
        )

        if not quote_df.empty:
            quote_data = quote_df.iloc[0].to_dict()
            edited_values = {}

            for field, value in quote_data.items():
                col1, col2 = st.columns([1, 4])
                with col1:
                    st.markdown(f"**{field}**")
                with col2:
                    if field == "quote_id":
                        st.text_input(label="", value=str(value), disabled=True, key=field)
                    else:
                        edited_values[field] = st.text_input(label="", value=str(value), key=field)

            if st.button("💾 Save Updated Quote"):
                try:
                    set_clause = ",\n  ".join([f"{k} = '{v}'" for k, v in edited_values.items()])
                    update_sql = f"""
                        MERGE INTO lrcatalog.agentic_underwriting.quotes AS target
                        USING (SELECT '{selected_quote_id}' AS quote_id) AS source
                        ON target.quote_id = source.quote_id
                        WHEN MATCHED THEN UPDATE SET
                          {set_clause}
                    """
                    _ = query_sql(update_sql, user_token)
                    st.success("Quote updated successfully!")
                except Exception as e:
                    st.error(f"Failed to update quote: {e}")
        else:
            st.warning("No matching quote found in `quotes` table.")

        # --- Back Button ---
        if st.button("🔙 Back to all quotes"):
            st.query_params.clear()

    else:
        # ----------------------------
        # Main Page with Quote List and Job Runner
        # ----------------------------
        st.markdown("## 🔍 Select a Quote to View Details")

        if st.button("🔄 Refresh List"):
            st.rerun()

        for quote_id in df["quote_id"].unique():
            st.markdown(
                f'<a class="quote-link" href="?quote_id={quote_id}" target="_self">🔗 {quote_id}</a>',
                unsafe_allow_html=True
            )

        # --- Trigger Job for Custom Quote ---
        st.markdown("---")
        st.markdown("## ⚙️ Run Custom Quote Job")

        input_quote_id = st.text_input("Enter Quote ID", placeholder="e.g. R9999")

        if st.button("▶️ Run Quote Job"):
            if not input_quote_id:
                st.warning("Please enter a quote ID.")
                st.stop()

            try:
                _ = w.jobs.run_now(
                    job_id=76987957769854,
                    job_parameters={"quote_id": input_quote_id}
                )
                st.success("✅ Job triggered. Please check back in a moment.")
            except Exception as e:
                st.error(f"Failed to start job: {e}")

except Exception as e:
    st.error(f"Error: {e}")