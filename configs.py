# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## 🔧 Environment Configuration Setup
# MAGIC This notebook initializes the catalog, schema, and configuration for underwriting agent notebooks.

# COMMAND ----------

# UC configurations
catalog = "lrcatalog"
schema = "agentic_underwriting"
user_path = "laurence.ryszka@databricks.com"

# Job configurations
job_path = "job_init"
repo_name = "agentic-motor-underwriting"
job_name = "job_init_config.yml"

# App configurations
app_init_path = "app_init"
app_folder_path = f"/Workspace/Users/{user_path}/{repo_name}/app/streamlit-data-app-obo-user"
sql_warehouse_id = "148ccb90800933a1"

# COMMAND ----------

# Create catalog and schema
print(f"🔄 Setting up environment...")
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

print(f"✅ Environment ready:")
print(f"   📁 Catalog: {catalog}")
print(f"   📊 Schema: {schema}")
print(f"   👤 User: {user_path}")
