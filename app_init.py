# Databricks notebook source
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("source_path", "")

# COMMAND ----------

warehouse_id = dbutils.widgets.get("warehouse_id")
job_id = dbutils.widgets.get("job_id")
source_path = dbutils.widgets.get("source_path")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App, AppDeployment, AppDeploymentMode, ComputeState
import json
    
# Initialize workspace client
w = WorkspaceClient()

# App configuration
app_name = "agent-underwriting-demo-app"

# COMMAND ----------

try:
    # Step 1: Create the app if it doesn't exist
    try:
        existing_app = w.apps.get(app_name)
        print(f"✅ App '{app_name}' already exists")
    except Exception:
        print(f"🔄 Creating new app: {app_name}")

        # Define resources that the app needs access to
        app_resources = [
            # SQL Warehouse access
            {
                "name": "sql-warehouse",
                "description": "SQL Warehouse for data access",
                "sql_warehouse": {
                    "id": warehouse_id
                }
            },
            # Job access
            {
                "name": "underwriting-job", 
                "description": "Job for processing quotes",
                "job": {
                    "id": job_id,
                    "permission": "CAN_MANAGE_RUN"
                }
            },
        ]

        app = App(
            name=app_name,
            description="Lakehouse app deployed from repository"
        )
        
        # Pass the App object to create()
        waiter = w.apps.create(app)
        created_app = waiter.result()
        print(f"✅ App created: {created_app.name}")
    
    # Step 2: Deploy the app
    print(f"🚀 Deploying app from: {source_path}")
    app_deployment = AppDeployment(
        source_code_path=source_path,
        mode=AppDeploymentMode.SNAPSHOT
    )
    deployment_waiter = w.apps.deploy(
        app_name,
        app_deployment
    )

    deployment = deployment_waiter.result()
    print(f"✅ Deployment successful! ID: {deployment.deployment_id}")
    
    # Step 3: Start the app
    print(f"🔍 Checking app compute status...")
    app_info = w.apps.get(app_name)
    
    # Check if compute_status exists and get the state
    if app_info.compute_status and app_info.compute_status.state:
        current_state = app_info.compute_status.state
        print(f"   Current compute state: {current_state}")
        
        # Only start if not already ACTIVE
        if current_state != ComputeState.ACTIVE:
            print(f"▶️ Starting app...")
            w.apps.start(app_name)
            print(f"✅ App start initiated")
        else:
            print(f"ℹ️ App is already running (ACTIVE state). Skipping start.")
    else:
        # If no compute status available, try to start anyway
        print(f"⚠️ Compute status not available. Attempting to start...")
        try:
            w.apps.start(app_name)
            print(f"✅ App start initiated")
        except Exception as start_error:
            print(f"⚠️ Start failed (app may already be running): {start_error}")
    
    # Step 4: Get app details
    app_info = w.apps.get(app_name)
    print(f"🌐 App is running at: {app_info.url}")
    print(f"🌐 App is using service principal ID: {app_info.service_principal_id}")
    
    app_result = {
        "status": "success",
        "app_url": app_info.url,
        "app_id": app_info.id,
        "sp_id": app_info.service_principal_id,
        "sp_name": app_info.service_principal_name
    }

    dbutils.notebook.exit(json.dumps(app_result))
    
except Exception as e:
    print(f"❌ Deployment failed: {e}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC
