# Databricks notebook source
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("user_path", "")
dbutils.widgets.text("repo_name", "")
dbutils.widgets.text("job_name", "")

# COMMAND ----------

# Get the values from configs
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
user_path = dbutils.widgets.get("user_path")
repo_name = dbutils.widgets.get("repo_name")
job_name = dbutils.widgets.get("job_name")

# COMMAND ----------

existing_cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# COMMAND ----------

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

# Load config
with open(job_name, 'r') as f:
    config = yaml.safe_load(f)

# Create job
w = WorkspaceClient()

notebook_path = config['notebook_path_template'].format(
    user=user_path, repo_name=repo_name
)

libraries = [{"pypi": {"package": lib['pypi']}} for lib in config['cluster']['libraries']]

job = w.jobs.create(
    name=f"{config['job_name']} - {user_path.split('@')[0]}",
    max_concurrent_runs=config['max_concurrent_runs'],
    tasks=[
        jobs.Task(
            task_key=config['task']['task_key'],
            notebook_task=jobs.NotebookTask(
                notebook_path=notebook_path,
                base_parameters={"catalog": catalog, "schema": schema, "quote_id": "R9999"},
                source=jobs.Source.WORKSPACE
            ),
            existing_cluster_id=existing_cluster_id
        )
    ],
    queue=jobs.QueueSettings(enabled=config['queue']['enabled'])
)

print(f"✅ Job created! ID: {job.job_id}")
print(f"🔗 URL: {w.config.host}/#job/{job.job_id}")



# COMMAND ----------

import json

job_info = {
    "status": "success",
    "job_id": job.job_id,  # This should be the ID of the job you created
    "job_url": f"{w.config.host}/#job/{job.job_id}"
}

dbutils.notebook.exit(json.dumps(job_info))
