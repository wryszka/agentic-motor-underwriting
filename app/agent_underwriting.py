import os
import time
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ResultData
from databricks.sdk.service.workspace import RunNotebookRequest

# Init Databricks client
w = WorkspaceClient()

# Set table and notebook config
catalog = "lrcatalog"
schema = "agentic_underwriting"
table_name = f"{catalog}.{schema}.agent_review"
notebook_path = "/Workspace/Users/laurence.ryszka@databricks.com/actuarial-pricing-demo/Agentic-Motor-underwriting/app"  # TODO: change to your actual notebook path

# Check env
assert os.getenv("SQL_WAREHOUSE_ID"), "Set SQL_WAREHOUSE_ID in app.yaml"

st.title("🚗 Agent Review Dashboard")

# Function to run SQL and return rows
def run_sql(query: str) -> list[list]:
    stmt = w.sql_statements.execute_statement(
        warehouse_id=os.getenv("SQL_WAREHOUSE_ID"),
        statement=query,
        wait_timeout=30
    )
    while stmt.status in {"PENDING", "RUNNING"}:
        time.sleep(1)
        stmt = w.sql_statements.get(stmt.statement_id)

    if stmt.status == "FAILED":
        st.error("SQL query failed.")
        return []

    result: ResultData = w.sql_statements.get_result(stmt.statement_id).result
    return result.data_array if result else []

# Load entries from agent_review
rows = run_sql(f"SELECT quote_id, description FROM {table_name}")
if not rows:
    st.warning("No quotes available for review.")
    st.stop()

# Quote selector
quote_options = {row[0]: row[1] for row in rows}
selected_quote = st.selectbox("Select quote ID:", options=quote_options.keys())
st.caption(f"📄 Reason for review: {quote_options[selected_quote]}")

# Run notebook
if st.button("▶️ Run Agent Review"):
    with st.spinner("Running agent notebook..."):
        run = w.jobs.run_now(
            run_name="AgentReviewStreamlitApp",
            notebook_task=RunNotebookRequest(
                notebook_path=notebook_path,
                base_parameters={"quote_id": selected_quote}
            )
        )
        while run.state.life_cycle_state in {"PENDING", "RUNNING"}:
            time.sleep(3)
            run = w.jobs.get_run(run.run_id)

        if run.state.result_state == "SUCCESS":
            st.success("Agent run completed.")
            result = run_sql(
                f"SELECT agent_output FROM {table_name} WHERE quote_id = '{selected_quote}'"
            )
            if result:
                st.subheader("🧠 Agent Output")
                st.markdown(result[0][0])
            else:
                st.info("No agent output found for this quote.")
        else:
            st.error(f"Notebook failed with state: {run.state.result_state}")