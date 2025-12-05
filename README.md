# Power BI Governance & Impact Analysis Solution 

## What It Does
This provides a quick and automated way to identify where and how specific fields, measures, and tables are used across Power BI reports in all workspaces by analyzing the Report Layer. It also breaks down the details of your models, reports, and dataflows for easy review, giving you an all-in-one **Power BI Governance** solution.

### Key Features:
- **Impact Analysis**: Fully understand the downstream impact of data model changes, ensuring you don’t accidentally break visuals or dashboards—especially when reports connected to a model span multiple workspaces.
- **Used and Unused Objects**: Identify which tables, columns, and measures are actively used and where. Equally as important, see what isn't used and can be safely removed from your model to save space and complexity.
- **Comprehensive Environment Overview**: Gain a clear, detailed view of your entire Power BI environment, including complete breakdowns of your models, reports, and dataflows and their dependencies. 
- **User-Friendly Output**: Results are presented in a Power BI model, making them easy to explore, analyze, and share with your team.

---
#### ✨ Recently Added Features

- **Workspace Selector** → Only want to run this against 1, 2, 10 workspaces? Now
a popup will allow you to choose which workspaces you run this against. Select All will still run against eveyrthing and a built-in timer ensures no selection will run against everything.
- **Unused Model Objects** → Identify model fields/measures not used in any visuals, measures, calculated columns, or relationships.  
- **Broken Visuals (with Page Links)** → See all broken visuals/filters and jump directly to the impacted report page.  
- **Report-Level Measures Inventory** → Surface report-only measures with full DAX and usage details.
- **New Report Layouts & Wireframe** → See where your visuals sit on the page with a wireframe layout - thanks to @stephbruno for this feature!
 ---
## Overview

The solution consists of:
- **GovernanceNotebook.py**: A Python notebook that extracts Power BI environment metadata (workspaces, datasets, reports, dataflows, etc.) and writes it to a Fabric Lakehouse
- **Semantic Link Power BI Governance Model.pbit**: A Power BI template for visualizing and analyzing the extracted metadata

## Prerequisites

- Access to a Microsoft Fabric workspace
- Permissions to create Fabric items (Environment, Lakehouse, Notebook)
- Power BI Desktop (for the template)

--

## Step 1: Import the Notebook

1. **Download the notebook**
   - Download `GovernanceNotebook.py` from this repository

2. **Import into your workspace**
   - In your Fabric workspace, click **Import** → **Notebook**
   - Choose **Upload from this computer**
   - Select the downloaded `GovernanceNotebook.py` file
   - Click **Open**

---

## Step 2: Create a Lakehouse

1. **Create a new Lakehouse**
   - Once in the notebook, click **Add lakehouse** on the left panel
   - Select **New lakehouse**
   - Give it a descriptive name (e.g., "PowerBIGovernance")
   - **Important**: Enable **Lakehouse schemas** checkbox
   - Click **Create**

---

## Step 3: Run the Notebook

### Configuration (Optional)

By default, the notebook is pre-configured with  defaults:
- **Lakehouse Schema**: `dbo` (the default schema)
- **Workspaces**: `["All"]` (scans all workspaces you have access to)
- **Parallel Workers**: `5` (number of parallel API calls)

You can modify these settings at the top of the notebook if needed:

```python
LAKEHOUSE_SCHEMA = "dbo"          # Schema name in your Lakehouse
WORKSPACE_NAMES = ["All"]         # ["All"] or ["Workspace1", "Workspace2"]
MAX_PARALLEL_WORKERS = 5          # 1-10 (higher = faster but more API load)
```
---

## Step 4: Open & Refresh The Power BI Model / Report Template

1. **Open the template**
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

## (Optional) Schedule the Notebook in a Pipeline

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

## Screenshots of Final Output
..
..

<img width="1235" alt="image" src="https://github.com/user-attachments/assets/805d3145-8290-4d84-8da2-bb27529bb050">
<img width="1259" alt="image" src="https://github.com/user-attachments/assets/54212360-8d0f-44c5-9337-db2cdd0fb5ee">
<img width="1240" alt="image" src="https://github.com/user-attachments/assets/488fc303-a9fa-4d4e-b0ce-c827fb440e83">
<img width="1259" alt="image" src="https://github.com/user-attachments/assets/9280e350-8714-40e5-8e09-d1de07faf5f5">
<img width="1221" alt="image" src="https://github.com/user-attachments/assets/e120c1bb-b52a-4197-aeb3-2a6ddbb67a9f">
<img width="1221" alt="image" src="https://github.com/user-attachments/assets/c9f5331d-8976-4f66-be76-5628e38e8d0f">
<img width="1241" alt="image" src="https://github.com/user-attachments/assets/9d814034-494d-478b-b231-f759d7eebeab">
