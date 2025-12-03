# Power BI Governance Solution - Setup Guide

This repository contains a comprehensive Power BI governance solution that extracts metadata from your Power BI environment using Microsoft Fabric and semantic-link-labs.

## Overview

The solution consists of:
- **GovernanceNotebook.py**: A Python notebook that extracts Power BI environment metadata (workspaces, datasets, reports, dataflows, etc.) and writes it to a Fabric Lakehouse
- **Semantic Link Power BI Governance Model.pbit**: A Power BI template for visualizing and analyzing the extracted metadata

## Prerequisites

- Access to a Microsoft Fabric workspace
- Permissions to create Fabric items (Environment, Lakehouse, Notebook)
- Power BI Desktop (for the template)

---

## Step 1: Create a Fabric Environment with semantic-link-labs

1. **Navigate to your Fabric workspace**
   - Go to https://app.fabric.microsoft.com
   - Select or create a workspace

2. **Create a new Environment**
   - Click **New** → **More options** → **Environment**
   - Give it a descriptive name (e.g., "Power BI Governance Environment")

3. **Add the semantic-link-labs library**
   - In the Environment settings, go to **Public libraries** on the left
   - Click **Add from PyPI**
   - Choose **Pip**
   - Search for: `semantic-link-labs` (exactly as written, lowercase with underscores)
   - The version will auto-fill - leave it as the default
   - Click **Add**

4. **Publish the Environment**
   - Click **Publish** at the top right
   - Click **Publish All**
   - Wait for the environment to finish publishing

---

## Step 2: Import the Notebook

1. **Download the notebook**
   - Download `GovernanceNotebook.py` from this repository

2. **Import into your workspace**
   - In your Fabric workspace, click **Import** → **Notebook**
   - Choose **Upload from this computer**
   - Select the downloaded `GovernanceNotebook.py` file
   - Click **Open**

---

## Step 3: Create a Lakehouse

1. **Create a new Lakehouse**
   - In the notebook view, click **Add lakehouse** on the left panel
   - Select **New lakehouse**
   - Give it a descriptive name (e.g., "PowerBIGovernance")
   - **Important**: Enable **Lakehouse schemas** checkbox
   - Click **Create**

2. **Verify the Lakehouse is attached**
   - You should see your Lakehouse appear in the left panel under "Lakehouse"

---

## Step 4: Assign the Environment to the Notebook

1. **Open the notebook** (if not already open)

2. **Change the Environment**
   - At the top of the notebook, locate the **Environment** dropdown
   - Click **Change environment**
   - Select the Environment you created in Step 1
   - Wait for the environment to be attached (this may take a moment)

---

## Step 5: Run the Notebook

### Configuration (Optional)

By default, the notebook is pre-configured with sensible defaults:
- **Lakehouse Schema**: `dbo` (the default schema)
- **Workspaces**: `["All"]` (scans all accessible workspaces)
- **Parallel Workers**: `5` (number of parallel API calls)

You can modify these settings at the top of the notebook if needed:

```python
LAKEHOUSE_SCHEMA = "dbo"          # Schema name in your Lakehouse
WORKSPACE_NAMES = ["All"]         # ["All"] or ["Workspace1", "Workspace2"]
MAX_PARALLEL_WORKERS = 5          # 1-10 (higher = faster but more API load)
```

### Running the Notebook

1. **Run all cells**
   - Click **Run all** at the top of the notebook
   - OR click the play button in each cell sequentially

2. **Monitor progress**
   - The notebook will display progress as it extracts metadata
   - Extraction time varies based on the number of workspaces and items
   - Expect 5-15 minutes for most environments

3. **Verify the output**
   - Once complete, check your Lakehouse for the following tables in the `dbo` schema:
     - `Workspaces`, `Datasets`, `Reports`, `Dataflows`, `FabricItems`
     - `DatasetSourcesInfo`, `DatasetRefreshHistory`
     - `DataflowSourcesInfo`, `DataflowRefreshHistory`, `DataflowDetail`
     - `ReportPages`, `Pages`, `Visuals`, `Bookmarks`, etc.
     - `ModelDetail`, `ModelDependencies`
     - `Apps`, `AppReports`

---

## Step 6: Schedule the Notebook in a Pipeline (Optional)

To automate regular metadata extraction:

1. **Create or open a Data Pipeline**
   - In your workspace, click **New** → **Data pipeline**
   - Give it a name (e.g., "Power BI Governance Extraction")

2. **Add the notebook activity**
   - In the pipeline canvas, search for "Notebook" activity
   - Drag it onto the canvas
   - Configure the activity:
     - **Workspace**: Select your workspace
     - **Notebook**: Select your governance notebook

3. **Schedule the pipeline**
   - Click **Schedule** at the top
   - Enable the schedule
   - Set your desired frequency (e.g., daily at 2 AM)
   - Click **Apply**

4. **Save and run**
   - Click **Run** to test immediately
   - Monitor the run status in the pipeline view

---

## Step 7: Connect the Power BI Template

1. **Download Power BI Desktop**
   - If you don't have it, download from https://powerbi.microsoft.com/desktop/

2. **Open the template**
   - Download `Semantic Link Power BI Governance Model.pbit` from this repository
   - Open it with Power BI Desktop

3. **Get the Lakehouse SQL Connection String**
   - Go back to your Fabric workspace
   - Find your Lakehouse in the workspace items list
   - Look for the item with type **SQL analytics endpoint** (same name as your Lakehouse)
   - Click on it to open the SQL analytics endpoint
   - At the bottom left, click **Copy SQL connection string**

4. **Enter connection parameters**
   - The template will prompt you for:
     - **SQL Connection String**: Paste the connection string from step 3
       - Format: `<workspace-id>.datawarehouse.fabric.microsoft.com`
       - Remove any `server=` or other prefixes if present
     - **Lakehouse Name**: Enter the exact name of your Lakehouse (e.g., "PowerBIGovernance")
   - Click **Load**

5. **Authenticate**
   - Choose **Organizational account** authentication
   - Sign in with your Microsoft/Azure credentials
   - Click **Connect**

6. **Wait for data to load**
   - Power BI will load all the metadata from your Lakehouse
   - This may take a few minutes depending on data volume

---

## Step 8: Publish and Schedule Refresh

1. **Publish the report**
   - In Power BI Desktop, click **File** → **Publish** → **Publish to Power BI**
   - Select your workspace
   - Click **Select**

2. **Configure scheduled refresh**
   - In the Power BI Service (https://app.powerbi.com)
   - Go to your workspace
   - Find the dataset (same name as your report)
   - Click **...** (More options) → **Settings**
   - Expand **Scheduled refresh**
   - Configure refresh frequency
   - Click **Apply**

3. **Create a coordinated schedule**
   - Schedule the notebook to run first (e.g., 2:00 AM)
   - Schedule the Power BI dataset refresh shortly after (e.g., 3:00 AM)
   - This ensures fresh data is available when the report refreshes

---

## Troubleshooting

### Notebook fails with "Module not found: sempy_labs"
- **Solution**: Ensure you've created an Environment with semantic-link-labs and assigned it to the notebook (Steps 1 & 4)

### "Schema not found" error
- **Solution**: Ensure you enabled "Lakehouse schemas" when creating the Lakehouse (Step 3)

### Power BI template can't connect
- **Solution**: 
  - Verify you copied the correct SQL connection string from the **SQL analytics endpoint** (not the Lakehouse itself)
  - Ensure the Lakehouse name matches exactly (case-sensitive)
  - Check that tables exist in the Lakehouse after running the notebook

### Tables are empty in Power BI
- **Solution**: 
  - Verify the notebook ran successfully and check for errors in the output
  - Confirm you have access to Power BI workspaces (the notebook can only extract data from workspaces you have access to)
  - Check that `WORKSPACE_NAMES` is set correctly in the notebook configuration

### Performance is slow
- **Solution**:
  - Adjust `MAX_PARALLEL_WORKERS` (higher values = faster but more API load)
  - Consider filtering to specific workspaces instead of `["All"]`
  - Run during off-peak hours to reduce API throttling

---

## Data Extracted

The solution extracts comprehensive metadata including:

### Environment Data
- Workspaces, Datasets, Reports, Dataflows
- Fabric Items (Lakehouses, Warehouses, Notebooks, etc.)
- Apps and App Reports
- Dataset data sources and refresh history
- Dataflow sources, refresh history, and lineage

### Report Metadata
- Report pages and visuals
- Bookmarks and custom visuals
- Report, page, and visual filters
- Visual objects and interactions
- Report-level measures

### Model Metadata
- Tables, columns, measures, hierarchies
- Calculated columns and calculation groups
- Partitions and relationships
- DAX expressions and dependencies

### Dataflow Details
- Power Query M expressions
- Query names and definitions
- Both Gen1 (Power BI) and Gen2 (Fabric) dataflows

---

## Configuration Options

### Workspace Filtering

To scan all workspaces (default):
```python
WORKSPACE_NAMES = ["All"]
```

To scan specific workspaces:
```python
WORKSPACE_NAMES = ["Marketing", "Finance", "Sales"]
```

### Schema Name

To use a custom schema (must exist or will be auto-created):
```python
LAKEHOUSE_SCHEMA = "governance"
```

### Performance Tuning

Adjust parallel workers (1-10):
```python
MAX_PARALLEL_WORKERS = 5  # Default: 5
```
- Lower values (1-3): Slower but gentler on APIs
- Higher values (7-10): Faster but may hit rate limits

---

## Support and Contributions

For issues, questions, or contributions, please visit the GitHub repository.

## License

This solution is provided as-is for Power BI governance and metadata extraction purposes.
