import os
import time
import streamlit as st
import pandas as pd
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# ======================
# --- CONFIG & CONSTANTS
# ======================
# Databricks OAuth-based config
cfg = Config()
w = WorkspaceClient()

# Warehouse is taken from cfg.warehouse_id
# Set your catalog/schema once here:
CATALOG = "lrcatalog"
SCHEMA = "agentic_underwriting"

# Tables
TABLE_AGENT_OUTPUT = "global_agent_output"  # replaces agent_review
TABLE_QUOTES_BY_PREFIX = {
    "M": "motor_quotes",
    "C": "commercial_quotes",
    "P": "property_quotes",
}

# Job IDs (set once here)
JOB_IDS = {
    "motor": 1234567890,       # ← put your real job id here
    "commercial": 2345678901,  # ← put your real job id here
    "property": 3456789012,    # ← put your real job id here
}

PREFIX_TO_LOB = {
    "M": "motor",
    "C": "commercial",
    "P": "property",
}

def fqtn(catalog: str, schema: str, table: str) -> str:
    """Fully Qualified Table Name"""
    return f"{catalog}.{schema}.{table}"

def get_lob_from_id(id_value: str) -> str | None:
    """Return 'motor'|'commercial'|'property' based on first letter M/C/P."""
    if not id_value:
        return None
    prefix = str(id_value).strip().upper()[:1]
    return PREFIX_TO_LOB.get(prefix)

def get_quotes_table_from_id(id_value: str) -> str | None:
    """Return quotes table name based on first letter M/C/P."""
    if not id_value:
        return None
    prefix = str(id_value).strip().upper()[:1]
    return TABLE_QUOTES_BY_PREFIX.get(prefix)

def escape_sql_literal(val: str) -> str:
    """Very light escaping for single quotes in SQL literals."""
    return str(val).replace("'", "''")

# =========================
# --- SQL Query Helper
# =========================
def query_sql(query: str, user_token: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        access_token=user_token
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall_arrow().to_pandas()

# =========================
# --- Streamlit Page Setup
# =========================
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

# =========================
# --- Auth & Query Params
# =========================
user_token = st.context.headers.get("X-Forwarded-Access-Token")
params = st.query_params
selected_quote_id = params.get("quote_id")

# =========================
# --- Load Agent Output
# =========================
try:
    df = query_sql(f"SELECT * FROM {fqtn(CATALOG, SCHEMA, TABLE_AGENT_OUTPUT)}", user_token)

    if selected_quote_id:
        # ----------------------------
        # Detail Page for Selected Quote
        # ----------------------------
        selected_quote_id = str(selected_quote_id)
        row = df[df['quote_id'] == selected_quote_id]

        if not row.empty:
            st.markdown(f"## 🧾 Agent Output: `{selected_quote_id}`")
            for k, v in row.iloc[0].to_dict().items():
                st.markdown(f"**{k}**: {v}")
        else:
            st.error(f"No agent output found for quote ID: {selected_quote_id}")

        # --- Editable Quote Section ---
        st.markdown("### ✏️ Edit Matching Quote Input")

        quotes_table = get_quotes_table_from_id(selected_quote_id)
        if quotes_table is None:
            st.warning("Couldn't infer line of business from ID. Use an ID starting with M, C, or P.")
        else:
            quote_df = query_sql(
                f"SELECT * FROM {fqtn(CATALOG, SCHEMA, quotes_table)} WHERE quote_id = '{escape_sql_literal(selected_quote_id)}'",
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
                            st.text_input(label="", value=str(value), disabled=True, key=f"disp_{field}")
                        else:
                            edited_values[field] = st.text_input(label="", value=str(value), key=f"edit_{field}")

                if st.button("💾 Save Updated Quote"):
                    try:
                        set_clause = ",\n  ".join([f"{k} = '{escape_sql_literal(v)}'" for k, v in edited_values.items()])
                        update_sql = f"""
                            MERGE INTO {fqtn(CATALOG, SCHEMA, quotes_table)} AS target
                            USING (SELECT '{escape_sql_literal(selected_quote_id)}' AS quote_id) AS source
                            ON target.quote_id = source.quote_id
                            WHEN MATCHED THEN UPDATE SET
                              {set_clause}
                        """
                        _ = query_sql(update_sql, user_token)
                        st.success("Quote updated successfully!")
                    except Exception as e:
                        st.error(f"Failed to update quote: {e}")
            else:
                st.warning(f"No matching quote found in `{quotes_table}`.")

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

        for quote_id in df["quote_id"].dropna().astype(str).unique():
            st.markdown(
                f'<a class="quote-link" href="?quote_id={quote_id}" target="_self">🔗 {quote_id}</a>',
                unsafe_allow_html=True
            )

        # --- Trigger Job for Custom Policy ---
        st.markdown("---")
        st.markdown("## ⚙️ Run Policy Job")

        input_policy_number = st.text_input("Enter Policy Number", placeholder="e.g. M12345 / C98765 / P54321")

        if st.button("▶️ Run Policy Job"):
            if not input_policy_number:
                st.warning("Please enter a policy number.")
                st.stop()

            lob = get_lob_from_id(input_policy_number)
            if not lob:
                st.error("Policy number must start with M, C, or P.")
                st.stop()

            job_id = JOB_IDS.get(lob)
            if not job_id:
                st.error(f"No job configured for line of business '{lob}'.")
                st.stop()

            try:
                _ = w.jobs.run_now(
                    job_id=job_id,
                    job_parameters={"policy_number": input_policy_number}  # <-- policy_number as requested
                )
                st.success("✅ Job triggered. Please check back in a moment.")
            except Exception as e:
                st.error(f"Failed to start job: {e}")

except Exception as e:
    st.error(f"Error: {e}")