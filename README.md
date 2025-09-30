# Agentic Motor Underwriting Pipeline

## Overview

This project demonstrates an end-to-end agentic workflow for motor insurance underwriting, showcasing how to build intelligent automation pipelines on the Databricks platform. The system combines synthetic data generation, automated underwriting decisions, and interactive data applications to create a comprehensive insurance processing solution.

**Key Features:**
- **Synthetic Data Generation**: Automated creation of realistic motor insurance quotes and customer data
- **Agentic Underwriting Pipeline**: AI-powered decision making for insurance quote evaluation
- **Price Scoring Model**: Machine learning models for risk assessment and pricing
- **Interactive Web Application**: Streamlit-based interface for quote review and management
- **End-to-End Automation**: Complete workflow from data generation to decision deployment

**Technology Stack:**
- **Databricks Model Serving**: Serving endpoint for hosting ML models
- **Unity Catalog**: Data governance and security layer
- **Databricks Jobs**: Orchestrated workflow execution
- **Databricks Apps**: Interactive web application hosting
- **Streamlit**: Frontend framework for data applications

## Folder Structure

agentic-motor-underwriting/

    📁 agents/
        └── Agent workflow definitions and business logic
    
    📁 app/
        └── Streamlit application for quote review and management
    
    📁 data/
        └── Data generation and processing utilities
    
    📄 [Configuration & Setup]
        ├── configs
        └── 1. Setup and data generation
    
    📄 [Core Workflows]
        ├── 2. Agentic Underwriting Pipeline
        └── 3. Price Scoring Model
    
    📄 [Deployment]
        ├── job_init
        ├── job_init_config.yml
        └── app_init
    
    📄 [Instructions]
        └── README.md

## Setup Instructions

### Prerequisites

- ✅ **Databricks Workspace** with Unity Catalog enabled
- ✅ **SQL Warehouse** for data processing
- ✅ **Appropriate permissions** for catalog/schema creation
- ✅ **Repository access** in Databricks Git Folder

### Configuration Parameters

Before running the project, update the following configuration parameters in the `configs` notebook:

#### 1. Unity Catalog Configurations

*UC configurations - CUSTOMISE THESE VALUES*

- catalog = "\<your_catalog_name>" # Replace with your catalog name
- schema = "\<agentic_underwriting>" # Schema name (can keep as-is)
- user_path = "\<your.email@company.com>" # Replace with your Databricks email

#### 2. Job Configurations

*Job configurations - OPTIONALLY CHANGE THESE VALUES*

- job_path = "job_init" # [Optional]: Notebook path for job initialization
- repo_name = "\<your-repo-name>" # **[Necessary]**: Replace with your repository name
- job_name = "job_init_config.yml" # [Optional]: Job configuration file name

#### 3. App Configurations

*App configurations - OPTIONALLY CHANGE THESE VALUES*
- app_init_path = "app_init" # [Optional] Notebook path for app initialization
- app_folder_path = f"/Workspace/Users/{user_path}/{repo_name}/app/streamlit-data-app-obo-user" # [Optional] Source code path for app deployment
- sql_warehouse_id = "\<your_warehouse_id>" # **[Necessary]**: Replace with your SQL Warehouse ID


#### 4. Environment Variables (in app.yaml)

When running the `1. Setup and data generation` notebook, these environment variables will be automatically configured:

**env**:

\- name: 'DATABRICKS_WAREHOUSE_ID'

value: '\<your_warehouse_id>'

\- name: 'DATABRICKS_SERVER_HOSTNAME'

value: '\<your-workspace.azuredatabricks.net>'

\- name: 'DATABRICKS_HTTP_PATH'

value: '/sql/1.0/warehouses/\<your_warehouse_id>'

\- name: 'DATABRICKS_JOB_ID'

value: '\<your_job_id>'


### How to Find Your Configuration Values

| Configuration | Location | Instructions |
|---------------|----------|-------------|
| **SQL Warehouse ID** | SQL Warehouses | Go to **SQL Warehouses** → Click your warehouse → Copy **ID** from URL or details |
| **Repository Name** | Repos | Go to **Repos** → Find your cloned repository → Use the repository folder name |


### Deployment Steps

| Step | Where | Purpose |
|------|----------|---------|
| 1 | `configs` | Update configuration parameters with your values |
| 2 | `1. Setup and data generation` | Run entire notebook to generate data and create agentic workflow |
| 2.1 |  | Generate synthetic data |
| 2.2 |  | Call `job_init` notebook to create and configure Databricks Jobs |
| 2.3 |  | Call `app_init` to deploy the interactive Streamlit application |
| 3 | Databricks App | Use the URL provided above and test the agentic workflow defined in `2. Agentic Underwriting Pipeline`|


### Permissions Required

- 🔐 **Catalog creation** permissions in Unity Catalog
- 🔐 **SQL Warehouse usage** permissions
- 🔐 **Job creation and management** permissions  
- 🔐 **App deployment** permissions
- 🔐 **Repository access** in Databricks Repos

### Troubleshooting

| Issue | Solution |
|-------|----------|
| Catalog creation fails | Check Unity Catalog permissions |
| SQL Warehouse connection errors | Verify warehouse ID and permissions |
| App deployment fails | Ensure service principal has required permissions |
| Job execution errors | Check job configuration and resource access |

---

For detailed step-by-step instructions, follow the numbered notebooks in sequence. 