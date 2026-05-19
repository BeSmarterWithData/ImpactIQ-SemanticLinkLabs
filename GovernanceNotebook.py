#!/usr/bin/env python
# coding: utf-8

# ## GovernanceNotebook
# 
# IMPORTANT: Before running this notebook:
# 1. Use Explorer to the left to "Add data items" and create a New Lakehouse or choose an Existing Lakehouse
# --------------------------------------------
# CONFIGURATION - No changes needed by default
# --------------------------------------------
LAKEHOUSE_SCHEMA = "dbo"          # <-- =Schema name in your attached Lakehouse - "dbo" is the typical default.
WORKSPACE_NAMES = ["All"]         # <-- ["All"] to scan and loop through all workspaces, or ["Workspace1", "Workspace2"] for specific workspaces (max 25)

# -----------------------------------
# PERFORMANCE SETTINGS
# -----------------------------------
# MAX_PARALLEL_WORKERS: Number of parallel API calls (1-20)
#     - Higher values = faster extraction but more API load
#     - Lower values = slower but gentler on API rate limits
#     - Recommended: 3-5 for most environments

MAX_PARALLEL_WORKERS = 10

# EXTRACT_MODEL_DEPENDENCIES: Extract measure/column/calc-item dependencies
#     - True = extract dependencies (slower, requires additional XMLA queries per model)
#     - False = skip dependency extraction for faster runs

EXTRACT_MODEL_DEPENDENCIES = True

# EXTRACT_REPORT_METADATA: Extract detailed report metadata via ReportWrapper
#     - True = extract pages, visuals, filters, bookmarks, etc. (slower, opens each report)
#     - False = skip report detail extraction, create empty tables only

EXTRACT_REPORT_METADATA = True

# In[0]:

# ================================
# CONFIGURATION (SHARED ACROSS ALL CELLS)
# ================================
# 
# IMPORTANT: Before running this notebook:
# 1. Attach a default Lakehouse to this notebook
# 2. Configure the settings below
# 
# LAKEHOUSE_SCHEMA: The schema name where tables will be written.
#     This defines the schema within your attached Lakehouse.
#     Must contain only alphanumeric characters and underscores.
#
# WORKSPACE_NAMES: List of workspace names to scan.
#     - ["All"] (default) - Scans all workspaces you have access to
#     - ["Workspace1"] - Scans a single workspace
#     - ["Workspace1", "Workspace2", "Workspace3"] - Scans multiple workspaces (up to 20)
#
# ================================

def install(package):
    try:
        from notebookutils import mssparkutils
        if getattr(mssparkutils.runtime.context.get(), "isForPipeline", False):
            print(f"[SKIP] Pipeline mode → not installing {package}")
            return
    except:
        pass
    
    get_ipython().run_line_magic("pip", f"install {package}")

install("semantic-link-labs")

import re

# Validate MAX_PARALLEL_WORKERS
if not isinstance(MAX_PARALLEL_WORKERS, int) or MAX_PARALLEL_WORKERS < 1 or MAX_PARALLEL_WORKERS > 20:
    raise ValueError("MAX_PARALLEL_WORKERS must be an integer between 1 and 20.")

# -----------------------------------tree
# CONFIGURATION VALIDATION
# -----------------------------------
# Validate lakehouse schema name
if not LAKEHOUSE_SCHEMA:
    raise ValueError("LAKEHOUSE_SCHEMA must be set! Please provide a valid schema name (alphanumeric and underscores only).")
    
if not re.match(r'^[a-zA-Z0-9_]+$', LAKEHOUSE_SCHEMA):
    raise ValueError(f"Invalid lakehouse schema name: '{LAKEHOUSE_SCHEMA}'. Must contain only alphanumeric characters and underscores.")

# Validate workspace names
if not isinstance(WORKSPACE_NAMES, list):
    raise ValueError("WORKSPACE_NAMES must be a list. Use ['All'] to scan all workspaces, or ['Workspace1', 'Workspace2'] for specific workspaces.")

if len(WORKSPACE_NAMES) == 0:
    raise ValueError("WORKSPACE_NAMES cannot be empty. Use ['All'] to scan all workspaces.")

if len(WORKSPACE_NAMES) > 25:
    raise ValueError("WORKSPACE_NAMES can contain at most 25 workspace names. Use ['All'] to scan all workspaces.")

# Check if scanning all workspaces (case-insensitive check for "All")
SCAN_ALL_WORKSPACES = (len(WORKSPACE_NAMES) == 1 and WORKSPACE_NAMES[0].lower() == "all")

print(f"Configuration loaded:")
print(f"  Lakehouse Schema: {LAKEHOUSE_SCHEMA}")
if SCAN_ALL_WORKSPACES:
    print(f"  Workspaces: All (scanning all accessible workspaces)")
else:
    print(f"  Workspaces: {WORKSPACE_NAMES}")
print(f"  Parallel Workers: {MAX_PARALLEL_WORKERS}")
print(f"  Extract Model Dependencies: {EXTRACT_MODEL_DEPENDENCIES}")
print(f"  Extract Report Metadata: {EXTRACT_REPORT_METADATA}")


# In[1]:


# ================================
# POWER BI ENVIRONMENT DETAIL EXTRACTOR
# ================================
# 
# This notebook extracts comprehensive Power BI environment metadata
# using the Fabric sempy library and REST APIs, mimicking the PowerShell
# script from:
# https://github.com/chris1642/Power-BI-Backup-Impact-Analysis-Governance-Solution
#
# EXTRACTED DATA (written to lakehouse tables):
# 1. Workspaces - workspace metadata with renamed columns
# 2. FabricItems - Fabric items (excluding Reports and SemanticModels)
# 3. Datasets - dataset metadata with renamed columns
# 4. DatasetSourcesInfo - dataset data sources
# 5. DatasetRefreshHistory - dataset refresh history
# 6. DatasetRefreshSchedule - dataset refresh schedule with day/time combinations
# 7. Dataflows - dataflow metadata with renamed columns
# 8. DataflowLineage - dataflow lineage (upstream dataflows)
# 9. DataflowSourcesInfo - dataflow data sources
# 10. DataflowRefreshHistory - dataflow refresh history
# 11. Reports - report metadata with renamed columns
# 12. ReportPages - report pages with renamed columns
# 13. Apps - Power BI apps
# 14. AppReports - reports within apps
#
# All column names are renamed to match the PowerShell script output.
#
# PERFORMANCE OPTIMIZATIONS:
# - Batch REST API calls where possible
# - Reuse single FabricRestClient instance
# - Use efficient pandas operations for data collection
# - Parallel processing with ThreadPoolExecutor for independent API calls
# ================================

# %pip install semantic-link-labs --quiet

import time
import re
import pandas as pd
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import sempy.fabric as fabric
from sempy.fabric import FabricRestClient

EXTRACTION_TIMESTAMP = datetime.now()
REPORT_DATE = EXTRACTION_TIMESTAMP.strftime("%Y-%m-%d")
start_time = time.time()

# -----------------------------------
# Logging helpers
# -----------------------------------
def log(msg):
    print(msg, flush=True)

def elapsed_min():
    return (time.time() - start_time) / 60

# Heartbeat
import threading
heartbeat_running = True
def heartbeat():
    while heartbeat_running:
        time.sleep(1000)
        print(f"[Heartbeat] Still running… elapsed {elapsed_min():.2f} min", flush=True)

threading.Thread(target=heartbeat, daemon=True).start()

# -----------------------------------
# Start banner
# -----------------------------------
log("="*80)
log("POWER BI ENVIRONMENT DETAIL EXTRACTION")
log(f"Started: {EXTRACTION_TIMESTAMP}")
log("="*80)

# ============================================
# AUTO-CREATE SCHEMA (LAKEHOUSE)
# ============================================
CATALOG = spark.sql("SELECT current_catalog()").first()[0]
log(f"Using catalog: {CATALOG}")

schema_name = f"{CATALOG}.{LAKEHOUSE_SCHEMA}"
log(f"Ensuring lakehouse schema exists: {schema_name}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
log(f"✓ Schema is ready: {schema_name}\n")

# ==============================================================  
# CLEAR ALL OUTPUT TABLES (ENSURES NO STALE DATA FROM PRIOR RUNS)
# ==============================================================
# This creates empty tables with correct schema for ALL cells upfront.
# Each cell then overwrites with actual data when it runs.
# If a cell is skipped, the table remains empty rather than stale.

ALL_TABLE_SCHEMAS = {
    # Cell 1 tables
    "Workspaces": {"WorkspaceId": "", "WorkspaceName": "", "WorkspaceType": "", "WorkspaceCapacityId": ""},
    "FabricItems": {"WorkspaceId": "", "WorkspaceName": "", "FabricItemID": "", "FabricItemType": "", "FabricItemName": "", "FabricItemDescription": ""},
    "Datasets": {"WorkspaceId": "", "WorkspaceName": "", "DatasetId": "", "DatasetName": "", "DatasetDescription": "", "DatasetWebUrl": "", "DatasetConfiguredBy": "", "DatasetIsRefreshable": "", "DatasetTargetStorageMode": "", "DatasetCreatedDate": ""},
    "DatasetSourcesInfo": {"WorkspaceId": "", "WorkspaceName": "", "DatasetId": "", "DatasetName": "", "DatasetDatasourceType": "", "DatasetDatasourceId": "", "DatasetDatasourceGatewayId": "", "DatasetDatasourceConnectionDetails": ""},
    "DatasetRefreshHistory": {"WorkspaceId": "", "WorkspaceName": "", "DatasetId": "", "DatasetName": "", "DatasetRefreshRequestId": "", "DatasetRefreshId": "", "DatasetRefreshStartTime": "", "DatasetRefreshEndTime": "", "DatasetRefreshStatus": "", "DatasetRefreshType": ""},
    "DatasetRefreshSchedule": {"WorkspaceId": "", "WorkspaceName": "", "DatasetId": "", "DatasetName": "", "DatasetRefreshScheduleEnabled": "", "DatasetRefreshScheduleLocalTimeZoneId": "", "DatasetRefreshScheduleNotifyOption": "", "DatasetRefreshScheduleDay": "", "DatasetRefreshScheduleTime": ""},
    "Dataflows": {"WorkspaceId": "", "WorkspaceName": "", "DataflowId": "", "DataflowName": "", "DataflowDescription": "", "DataflowConfiguredBy": "", "DataflowModifiedBy": "", "DataflowModifiedDateTime": "", "DataflowJsonURL": "", "DataflowGeneration": ""},
    "DataflowLineage": {"WorkspaceId": "", "WorkspaceName": "", "DataflowId": "", "DataflowName": "", "DatasetId": "", "DatasetName": ""},
    "DataflowSourcesInfo": {"WorkspaceId": "", "WorkspaceName": "", "DataflowId": "", "DataflowName": "", "DataflowDatasourceType": "", "DataflowDatasourceId": "", "DataflowDatasourceGatewayId": "", "DataflowDatasourceConnectionDetails": ""},
    "DataflowRefreshHistory": {"WorkspaceId": "", "WorkspaceName": "", "DataflowId": "", "DataflowName": "", "DataflowRefreshRequestId": "", "DataflowRefreshId": "", "DataflowRefreshStartTime": "", "DataflowRefreshEndTime": "", "DataflowRefreshStatus": "", "DataflowRefreshType": "", "DataflowErrorInfo": ""},
    "Reports": {"WorkspaceId": "", "WorkspaceName": "", "ReportId": "", "ReportName": "", "ReportDescription": "", "ReportWebUrl": "", "ReportEmbedUrl": "", "ReportType": "", "DatasetId": "", "DatasetName": ""},
    "ReportPages": {"WorkspaceId": "", "WorkspaceName": "", "ReportId": "", "ReportName": "", "PageName": "", "PageDisplayName": "", "PageOrder": 0},
    "Apps": {"AppId": "", "AppName": "", "AppLastUpdate": "", "AppDescription": "", "AppPublishedBy": "", "AppWorkspaceId": "", "WorkspaceName": ""},
    "AppReports": {"AppId": "", "AppName": "", "AppReportId": "", "AppReportType": "", "ReportName": "", "AppReportWebUrl": "", "AppReportEmbedUrl": "", "AppReportIsOwnedByMe": "", "AppReportDatasetId": "", "ReportId": "", "WorkspaceName": ""},
    # Cell 2 tables
    "ModelDetail": {"Type": "", "Table": "", "Name": "", "FormatString": "", "DisplayFolder": "", "Description": "", "IsHidden": "", "TableStorageMode": "", "Expression": "", "ModelAsOfDate": "", "ModelName": "", "ModelID": "", "WorkspaceName": "", "RelationshipFromTable": "", "RelationshipFromColumn": "", "RelationshipToTable": "", "RelationshipToColumn": "", "RelationshipStatus": "", "RelationshipFromCardinality": "", "RelationshipToCardinality": "", "RelationshipCrossFilteringBehavior": ""},
    "ModelDependencies": {"ObjectName": "", "ObjectType": "", "DependsOn": "", "DependsOnType": "", "ModelAsOfDate": "", "ModelName": "", "ModelID": "", "WorkspaceName": ""},
    # Cell 3 tables
    "Connections": {"ReportID": "", "ModelID": "", "ReportDate": "", "ReportName": "", "Type": "", "ServerName": "", "WorkspaceName": ""},
    "Pages": {"ReportName": "", "ReportID": "", "ModelID": "", "Id": "", "Name": "", "Number": 0, "Width": 0, "Height": 0, "HiddenFlag": "", "VisualCount": 0, "Type": "", "DisplayOption": "", "DataVisualCount": 0, "VisibleVisualCount": 0, "PageFilterCount": 0, "ReportDate": "", "WorkspaceName": ""},
    "Visuals": {"ReportName": "", "ReportID": "", "ModelID": "", "PageName": "", "PageId": "", "Id": "", "Name": "", "Type": "", "DisplayType": "", "Title": "", "SubTitle": "", "AltText": "", "TabOrder": 0, "CustomVisualFlag": "", "HiddenFlag": "", "X": 0.0, "Y": 0.0, "Z": 0, "Width": 0.0, "Height": 0.0, "ObjectCount": 0, "VisualFilterCount": 0, "DataLimit": 0, "Divider": "", "RowSubTotals": "", "ColumnSubTotals": "", "DataVisual": "", "HasSparkline": "", "ParentGroup": "", "ReportDate": "", "WorkspaceName": ""},
    "Bookmarks": {"ReportName": "", "ReportID": "", "ModelID": "", "Name": "", "Id": "", "PageName": "", "PageId": "", "VisualId": "", "VisualHiddenFlag": "", "SuppressData": "", "CurrentPageSelected": "", "ApplyVisualDisplayState": "", "ApplyToAllVisuals": "", "ReportDate": "", "WorkspaceName": ""},
    "CustomVisuals": {"ReportName": "", "ReportID": "", "ModelID": "", "Name": "", "ReportDate": "", "WorkspaceName": ""},
    "ReportFilters": {"ReportName": "", "ReportID": "", "ModelID": "", "displayName": "", "TableName": "", "ObjectName": "", "ObjectType": "", "FilterType": "", "HiddenFilter": "", "LockedFilter": "", "HowCreated": "", "Used": "", "ReportDate": "", "WorkspaceName": ""},
    "PageFilters": {"ReportName": "", "ReportID": "", "ModelID": "", "PageId": "", "PageName": "", "displayName": "", "TableName": "", "ObjectName": "", "ObjectType": "", "FilterType": "", "HiddenFilter": "", "LockedFilter": "", "HowCreated": "", "Used": "", "ReportDate": "", "WorkspaceName": ""},
    "VisualFilters": {"ReportName": "", "ReportID": "", "ModelID": "", "PageName": "", "PageId": "", "VisualId": "", "TableName": "", "ObjectName": "", "ObjectType": "", "FilterType": "", "HiddenFilter": "", "LockedFilter": "", "displayName": "", "HowCreated": "", "Used": "", "ReportDate": "", "WorkspaceName": ""},
    "VisualObjects": {"ReportName": "", "ReportID": "", "ModelID": "", "PageName": "", "PageId": "", "VisualId": "", "VisualName": "", "VisualType": "", "CustomVisualFlag": "", "TableName": "", "ObjectName": "", "ObjectType": "", "Source": "", "displayName": "", "ImplicitMeasure": "", "Sparkline": "", "VisualCalc": "", "Format": "", "ReportDate": "", "WorkspaceName": ""},
    "ReportLevelMeasures": {"ReportName": "", "ReportID": "", "ModelID": "", "TableName": "", "ObjectName": "", "ObjectType": "", "Expression": "", "HiddenFlag": "", "FormatString": "", "DataType": "", "DataCategory": "", "ReportDate": "", "WorkspaceName": ""},
    "VisualInteractions": {"ReportName": "", "ReportID": "", "ModelID": "", "PageName": "", "PageId": "", "SourceVisualID": "", "TargetVisualID": "", "SourceVisualName": "", "TargetVisualName": "", "TypeID": "", "Type": "", "ReportDate": "", "WorkspaceName": ""},
    # Cell 4 tables
    "DataflowDetail": {"DataflowId": "", "DataflowName": "", "QueryName": "", "Query": "", "ReportDate": "", "WorkspaceName": "", "WorkspaceNameDataflowName": ""},
}

log("Clearing all output tables (removing stale data from prior runs)...")

def _clear_table(tbl_name_schema):
    tbl_name, tbl_schema = tbl_name_schema
    full_name = f"{CATALOG}.{LAKEHOUSE_SCHEMA}.{tbl_name}"
    try:
        empty_df = spark.createDataFrame(pd.DataFrame([tbl_schema])).filter("1=0")
        empty_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(full_name)
    except Exception as e:
        log(f"  Warning: Could not clear {tbl_name}: {e}")

from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
    executor.map(_clear_table, ALL_TABLE_SCHEMAS.items())

log(f"✓ All {len(ALL_TABLE_SCHEMAS)} tables cleared\n")

workspaces_info = []
fabric_items_info = []
datasets_info = []
dataset_sources_info = []
dataset_refresh_history = []
dataset_refresh_schedule = []
dataflows_info = []
dataflow_lineage = []
dataflow_sources_info = []
dataflow_refresh_history = []
reports_info = []
report_pages_info = []
apps_info = []
reports_in_app_info = []

# Lookup tables
dataset_name_lookup = {}
dataflow_name_lookup = {}

# ==============================================================  
# SAMPLE ROWS FOR EMPTY TABLE CREATION
# ==============================================================

SAMPLE_ROWS = {
    "Workspaces": {"WorkspaceId": "", "WorkspaceName": "", "WorkspaceType": "", "WorkspaceCapacityId": ""},
    "FabricItems": {"WorkspaceId": "", "WorkspaceName": "", "FabricItemID": "", "FabricItemType": "", "FabricItemName": "", "FabricItemDescription": ""},
    "Datasets": {"WorkspaceId": "", "WorkspaceName": "", "DatasetId": "", "DatasetName": "", "DatasetDescription": "", "DatasetWebUrl": "", "DatasetConfiguredBy": "", "DatasetIsRefreshable": "", "DatasetTargetStorageMode": "", "DatasetCreatedDate": ""},
    "DatasetSourcesInfo": {"WorkspaceId": "", "WorkspaceName": "", "DatasetId": "", "DatasetName": "", "DatasetDatasourceType": "", "DatasetDatasourceId": "", "DatasetDatasourceGatewayId": "", "DatasetDatasourceConnectionDetails": ""},
    "DatasetRefreshHistory": {"WorkspaceId": "", "WorkspaceName": "", "DatasetId": "", "DatasetName": "", "DatasetRefreshRequestId": "", "DatasetRefreshId": "", "DatasetRefreshStartTime": "", "DatasetRefreshEndTime": "", "DatasetRefreshStatus": "", "DatasetRefreshType": ""},
    "DatasetRefreshSchedule": {"WorkspaceId": "", "WorkspaceName": "", "DatasetId": "", "DatasetName": "", "DatasetRefreshScheduleEnabled": "", "DatasetRefreshScheduleLocalTimeZoneId": "", "DatasetRefreshScheduleNotifyOption": "", "DatasetRefreshScheduleDay": "", "DatasetRefreshScheduleTime": ""},
    "Dataflows": {"WorkspaceId": "", "WorkspaceName": "", "DataflowId": "", "DataflowName": "", "DataflowDescription": "", "DataflowConfiguredBy": "", "DataflowModifiedBy": "", "DataflowModifiedDateTime": "", "DataflowJsonURL": "", "DataflowGeneration": ""},
    "DataflowLineage": {"WorkspaceId": "", "WorkspaceName": "", "DataflowId": "", "DataflowName": "", "DatasetId": "", "DatasetName": ""},
    "DataflowSourcesInfo": {"WorkspaceId": "", "WorkspaceName": "", "DataflowId": "", "DataflowName": "", "DataflowDatasourceType": "", "DataflowDatasourceId": "", "DataflowDatasourceGatewayId": "", "DataflowDatasourceConnectionDetails": ""},
    "DataflowRefreshHistory": {"WorkspaceId": "", "WorkspaceName": "", "DataflowId": "", "DataflowName": "", "DataflowRefreshRequestId": "", "DataflowRefreshId": "", "DataflowRefreshStartTime": "", "DataflowRefreshEndTime": "", "DataflowRefreshStatus": "", "DataflowRefreshType": "", "DataflowErrorInfo": ""},
    "Reports": {"WorkspaceId": "", "WorkspaceName": "", "ReportId": "", "ReportName": "", "ReportDescription": "", "ReportWebUrl": "", "ReportEmbedUrl": "", "ReportType": "", "DatasetId": "", "DatasetName": ""},
    "ReportPages": {"WorkspaceId": "", "WorkspaceName": "", "ReportId": "", "ReportName": "", "PageName": "", "PageDisplayName": "", "PageOrder": 0},
    "Apps": {"AppId": "", "AppName": "", "AppLastUpdate": "", "AppDescription": "", "AppPublishedBy": "", "AppWorkspaceId": "", "WorkspaceName": ""},
    "AppReports": {"AppId": "", "AppName": "", "AppReportId": "", "AppReportType": "", "ReportName": "", "AppReportWebUrl": "", "AppReportEmbedUrl": "", "AppReportIsOwnedByMe": "", "AppReportDatasetId": "", "ReportId": "", "WorkspaceName": ""}
}

# ==============================================================  
# HELPER FUNCTIONS
# ==============================================================

def safe_get(row, column, default=""):
    """Safely get value from row"""
    try:
        val = row.get(column, default)
        return val if val is not None else default
    except Exception:
        return default

def serialize_json(obj):
    """Serialize object to JSON if non-empty, otherwise return empty string"""
    if obj:
        return json.dumps(obj)
    return ""

# ==============================================================  
# PARALLEL API HELPERS FOR PERFORMANCE
# ==============================================================
# These helpers enable parallel fetching of dataset/dataflow details
# which significantly reduces total extraction time.

# Use the configured parallel worker setting
MAX_WORKERS = MAX_PARALLEL_WORKERS

def fetch_dataset_details(client, ws_id, ws_name, dataset_id, dataset_name):
    """Fetch dataset sources, refresh history, and refresh schedule in parallel"""
    sources = []
    refreshes = []
    schedules = []
    errors = []
    
    # Fetch dataset sources
    try:
        datasources_url = f"v1.0/myorg/groups/{ws_id}/datasets/{dataset_id}/datasources"
        response = client.get(datasources_url)
        if response.status_code == 200:
            for datasource in response.json().get('value', []):
                sources.append({
                    "WorkspaceId": ws_id,
                    "WorkspaceName": ws_name,
                    "DatasetId": dataset_id,
                    "DatasetName": dataset_name,
                    "DatasetDatasourceType": datasource.get("datasourceType", ""),
                    "DatasetDatasourceId": datasource.get("datasourceId", ""),
                    "DatasetDatasourceGatewayId": datasource.get("gatewayId", ""),
                    "DatasetDatasourceConnectionDetails": serialize_json(datasource.get("connectionDetails"))
                })
    except Exception as e:
        errors.append(f"datasources: {e}")
    
    # Fetch dataset refresh history
    try:
        refresh_url = f"v1.0/myorg/groups/{ws_id}/datasets/{dataset_id}/refreshes"
        response = client.get(refresh_url)
        if response.status_code == 200:
            for refresh in response.json().get('value', []):
                refreshes.append({
                    "WorkspaceId": ws_id,
                    "WorkspaceName": ws_name,
                    "DatasetId": dataset_id,
                    "DatasetName": dataset_name,
                    "DatasetRefreshRequestId": refresh.get("requestId", ""),
                    "DatasetRefreshId": refresh.get("id", ""),
                    "DatasetRefreshStartTime": refresh.get("startTime", ""),
                    "DatasetRefreshEndTime": refresh.get("endTime", ""),
                    "DatasetRefreshStatus": refresh.get("status", ""),
                    "DatasetRefreshType": refresh.get("refreshType", "")
                })
    except Exception as e:
        errors.append(f"refresh history: {e}")
    
    # Fetch dataset refresh schedule
    try:
        schedule_url = f"v1.0/myorg/groups/{ws_id}/datasets/{dataset_id}/refreshSchedule"
        response = client.get(schedule_url)
        if response.status_code == 200:
            schedule_data = response.json()
            
            # Get base properties
            enabled = schedule_data.get("enabled", False)
            timezone = schedule_data.get("localTimeZoneId", "")
            notify_option = schedule_data.get("notifyOption", "")
            
            # Get days and times arrays, with fallback to [None] if empty/missing
            days = schedule_data.get("days", [])
            if not days:
                days = [None]
            
            times = schedule_data.get("times", [])
            if not times:
                times = [None]
            
            # Create separate rows for each day-time combination (cross join)
            for day in days:
                for time in times:
                    schedules.append({
                        "WorkspaceId": ws_id,
                        "WorkspaceName": ws_name,
                        "DatasetId": dataset_id,
                        "DatasetName": dataset_name,
                        "DatasetRefreshScheduleEnabled": str(bool(enabled)),
                        "DatasetRefreshScheduleLocalTimeZoneId": timezone,
                        "DatasetRefreshScheduleNotifyOption": notify_option,
                        "DatasetRefreshScheduleDay": day if day else "",
                        "DatasetRefreshScheduleTime": time if time else ""
                    })
    except Exception as e:
        # Log error but continue - not all datasets have refresh schedules configured
        errors.append(f"refresh schedule: {e}")
    
    return sources, refreshes, schedules, errors

def fetch_dataflow_details(client, ws_id, ws_name, dataflow_id, dataflow_name):
    """Fetch dataflow sources and refresh history in parallel"""
    sources = []
    refreshes = []
    errors = []
    
    # Fetch dataflow sources
    try:
        sources_url = f"v1.0/myorg/groups/{ws_id}/dataflows/{dataflow_id}/datasources"
        response = client.get(sources_url)
        if response.status_code == 200:
            for source in response.json().get('value', []):
                sources.append({
                    "WorkspaceId": ws_id,
                    "WorkspaceName": ws_name,
                    "DataflowId": dataflow_id,
                    "DataflowName": dataflow_name,
                    "DataflowDatasourceType": source.get("datasourceType", ""),
                    "DataflowDatasourceId": source.get("datasourceId", ""),
                    "DataflowDatasourceGatewayId": source.get("gatewayId", ""),
                    "DataflowDatasourceConnectionDetails": serialize_json(source.get("connectionDetails"))
                })
    except Exception as e:
        errors.append(f"datasources: {e}")
    
    # Fetch dataflow refresh history (transactions)
    try:
        refresh_url = f"v1.0/myorg/groups/{ws_id}/dataflows/{dataflow_id}/transactions"
        response = client.get(refresh_url)
        if response.status_code == 200:
            for refresh in response.json().get('value', []):
                refreshes.append({
                    "WorkspaceId": ws_id,
                    "WorkspaceName": ws_name,
                    "DataflowId": dataflow_id,
                    "DataflowName": dataflow_name,
                    "DataflowRefreshRequestId": refresh.get("requestId", ""),
                    "DataflowRefreshId": refresh.get("id", ""),
                    "DataflowRefreshStartTime": refresh.get("startTime", ""),
                    "DataflowRefreshEndTime": refresh.get("endTime", ""),
                    "DataflowRefreshStatus": refresh.get("status", ""),
                    "DataflowRefreshType": refresh.get("refreshType", ""),
                    "DataflowErrorInfo": serialize_json(refresh.get("errorInfo"))
                })
    except Exception as e:
        errors.append(f"refresh history: {e}")
    
    return sources, refreshes, errors

# ==============================================================  
# GET WORKSPACES
# ==============================================================

log("Fetching workspaces...")
workspaces_df = fabric.list_workspaces()

if not SCAN_ALL_WORKSPACES:
    workspaces_df = workspaces_df[workspaces_df["Name"].isin(WORKSPACE_NAMES)]
    if workspaces_df.empty:
        raise ValueError(f"No workspaces found matching: {WORKSPACE_NAMES}")
    log(f"Filtering to workspaces: {WORKSPACE_NAMES}")

log(f"Workspace count: {len(workspaces_df)}")

# Build workspaces_info with renamed columns
for _, ws_row in workspaces_df.iterrows():
    workspaces_info.append({
        "WorkspaceId": safe_get(ws_row, "Id"),
        "WorkspaceName": safe_get(ws_row, "Name"),
        "WorkspaceType": safe_get(ws_row, "Type"),
        "WorkspaceCapacityId": safe_get(ws_row, "Capacity Id")
    })

log(f"✓ Workspaces collected: {len(workspaces_info)}\n")

# ==============================================================  
# EXTRACT ENVIRONMENT METADATA
# ==============================================================

# Create a single REST client instance to reuse
client = FabricRestClient()

for ws_info in workspaces_info:
    ws_name = ws_info["WorkspaceName"]
    ws_id = ws_info["WorkspaceId"]
    
    log(f"\nProcessing workspace: {ws_name} | Elapsed: {elapsed_min():.2f} min")

    # -------------------- DATASETS (with parallel detail fetching) --------------------
    try:
        log(f"  Fetching datasets...")
        datasets_df = fabric.list_datasets(workspace=ws_name)
        
        if datasets_df is not None and not datasets_df.empty:
            log(f"  Datasets found: {len(datasets_df)}")
            
            # Collect dataset basic info first
            dataset_tasks = []
            for _, ds_row in datasets_df.iterrows():
                dataset_id = safe_get(ds_row, "Dataset ID")
                dataset_name = safe_get(ds_row, "Dataset Name")
                
                # Store in lookup
                dataset_name_lookup[dataset_id] = dataset_name
                
                datasets_info.append({
                    "WorkspaceId": ws_id,
                    "WorkspaceName": ws_name,
                    "DatasetId": dataset_id,
                    "DatasetName": dataset_name,
                    "DatasetDescription": safe_get(ds_row, "Description"),
                    "DatasetWebUrl": safe_get(ds_row, "Web URL"),
                    "DatasetConfiguredBy": safe_get(ds_row, "Configured By"),
                    "DatasetIsRefreshable": str(bool(safe_get(ds_row, "Is Refreshable", False))),
                    "DatasetTargetStorageMode": safe_get(ds_row, "Target Storage Mode"),
                    "DatasetCreatedDate": safe_get(ds_row, "Created Date")
                })
                
                dataset_tasks.append((dataset_id, dataset_name))
            
            # Fetch dataset details in parallel
            log(f"  Fetching dataset details in parallel (max {MAX_WORKERS} workers)...")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(fetch_dataset_details, client, ws_id, ws_name, ds_id, ds_name): (ds_id, ds_name)
                    for ds_id, ds_name in dataset_tasks
                }
                for future in as_completed(futures):
                    try:
                        sources, refreshes, schedules, errors = future.result()
                        dataset_sources_info.extend(sources)
                        dataset_refresh_history.extend(refreshes)
                        dataset_refresh_schedule.extend(schedules)
                        if errors:
                            ds_id, ds_name = futures[future]
                            for err in errors:
                                log(f"    Warning ({ds_name}): {err}")
                    except Exception as e:
                        ds_id, ds_name = futures[future]
                        log(f"    Error fetching details for {ds_name}: {e}")
        else:
            log(f"  No datasets found")
            
    except Exception as e:
        log(f"  ERROR fetching datasets: {e}")

    # -------------------- DATAFLOWS (with parallel detail fetching) --------------------
    try:
        log(f"  Fetching dataflows...")
        dataflows_url = f"v1.0/myorg/groups/{ws_id}/dataflows"
        response = client.get(dataflows_url)
        
        if response.status_code == 200:
            dataflows = response.json().get('value', [])
            log(f"  Dataflows found: {len(dataflows)}")
            
            # Collect dataflow basic info first
            dataflow_tasks = []
            for dataflow in dataflows:
                dataflow_id = dataflow.get("objectId", "")
                dataflow_name = dataflow.get("name", "")
                
                # Store in lookup
                if dataflow_id:
                    dataflow_name_lookup[dataflow_id] = dataflow_name
                
                dataflows_info.append({
                    "WorkspaceId": ws_id,
                    "WorkspaceName": ws_name,
                    "DataflowId": dataflow_id,
                    "DataflowName": dataflow_name,
                    "DataflowDescription": dataflow.get("description", ""),
                    "DataflowConfiguredBy": dataflow.get("configuredBy", ""),
                    "DataflowModifiedBy": dataflow.get("modifiedBy", ""),
                    "DataflowModifiedDateTime": dataflow.get("modifiedDateTime", ""),
                    "DataflowJsonURL": dataflow.get("modelUrl", ""),
                    "DataflowGeneration": dataflow.get("generation", "")
                })
                
                dataflow_tasks.append((dataflow_id, dataflow_name))
            
            # Fetch dataflow details in parallel
            if dataflow_tasks:
                log(f"  Fetching dataflow details in parallel (max {MAX_WORKERS} workers)...")
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = {
                        executor.submit(fetch_dataflow_details, client, ws_id, ws_name, df_id, df_name): (df_id, df_name)
                        for df_id, df_name in dataflow_tasks
                    }
                    for future in as_completed(futures):
                        try:
                            sources, refreshes, errors = future.result()
                            dataflow_sources_info.extend(sources)
                            dataflow_refresh_history.extend(refreshes)
                            if errors:
                                df_id, df_name = futures[future]
                                for err in errors:
                                    log(f"    Warning ({df_name}): {err}")
                        except Exception as e:
                            df_id, df_name = futures[future]
                            log(f"    Error fetching details for {df_name}: {e}")
        else:
            log(f"  No dataflows found")
    except Exception as e:
        log(f"  ERROR fetching dataflows: {e}")

    # -------------------- FABRIC ITEMS --------------------
    try:
        log(f"  Fetching Fabric items...")
        items_url = f"v1/workspaces/{ws_id}/items"
        response = client.get(items_url)
        
        if response.status_code == 200:
            items = response.json().get('value', [])
            # Filter out Reports and SemanticModels as they're handled separately
            filtered_items = [item for item in items if item.get('type') not in ['Report', 'SemanticModel']]
            
            log(f"  Fabric items found: {len(filtered_items)}")
            
            for item in filtered_items:
                fabric_items_info.append({
                    "WorkspaceId": ws_id,
                    "WorkspaceName": ws_name,
                    "FabricItemID": item.get("id", ""),
                    "FabricItemType": item.get("type", ""),
                    "FabricItemName": item.get("displayName", ""),
                    "FabricItemDescription": item.get("description", "")
                })
        else:
            log(f"  No Fabric items found")
    except Exception as e:
        log(f"  ERROR fetching Fabric items: {e}")

    # -------------------- REPORTS (with parallel page fetching) --------------------
    try:
        log(f"  Fetching reports...")
        reports_df = fabric.list_reports(workspace=ws_name)
        
        if reports_df is not None and not reports_df.empty:
            log(f"  Reports found: {len(reports_df)}")
            
            # Collect report info and page tasks
            page_tasks = []
            for _, rpt_row in reports_df.iterrows():
                report_id = safe_get(rpt_row, "Id")
                report_name = safe_get(rpt_row, "Name")
                dataset_id = safe_get(rpt_row, "Dataset Id")
                
                # Get dataset name from lookup
                dataset_name = dataset_name_lookup.get(dataset_id, "Unknown Dataset")
                
                reports_info.append({
                    "WorkspaceId": ws_id,
                    "WorkspaceName": ws_name,
                    "ReportId": report_id,
                    "ReportName": report_name,
                    "ReportDescription": safe_get(rpt_row, "Description"),
                    "ReportWebUrl": safe_get(rpt_row, "Web URL"),
                    "ReportEmbedUrl": safe_get(rpt_row, "Embed URL"),
                    "ReportType": safe_get(rpt_row, "Report Type"),
                    "DatasetId": dataset_id,
                    "DatasetName": dataset_name
                })
                
                page_tasks.append((report_id, report_name))
            
            # Fetch all report pages in parallel
            def fetch_report_pages(task):
                rpt_id, rpt_name = task
                pages = []
                try:
                    pages_url = f"v1.0/myorg/groups/{ws_id}/reports/{rpt_id}/pages"
                    pages_response = client.get(pages_url)
                    if pages_response.status_code == 200:
                        for page in pages_response.json().get('value', []):
                            pages.append({
                                "WorkspaceId": ws_id,
                                "WorkspaceName": ws_name,
                                "ReportId": rpt_id,
                                "ReportName": rpt_name,
                                "PageName": page.get("name", ""),
                                "PageDisplayName": page.get("displayName", ""),
                                "PageOrder": page.get("order", 0)
                            })
                except Exception as e:
                    log(f"    ERROR fetching pages for {rpt_name}: {e}")
                return pages
            
            if page_tasks:
                log(f"  Fetching report pages in parallel (max {MAX_WORKERS} workers)...")
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    for pages_result in executor.map(fetch_report_pages, page_tasks):
                        report_pages_info.extend(pages_result)
        else:
            log(f"  No reports found")
            
    except Exception as e:
        log(f"  ERROR fetching reports: {e}")

    log(f"✓ Finished workspace: {ws_name}")

# ==============================================================  
# APPS AND APP REPORTS
# ==============================================================

log("\n" + "="*80)
log("Fetching Apps and App Reports")
log("="*80)

try:
    apps_url = "v1.0/myorg/apps"
    response = client.get(apps_url)
    
    if response.status_code == 200:
        apps = response.json().get('value', [])
        log(f"Apps found: {len(apps)}")
        
        # Filter to only apps in our workspaces (create list once)
        workspace_ids = [ws['WorkspaceId'] for ws in workspaces_info]
        # Create workspace ID to name lookup
        workspace_name_lookup = {ws['WorkspaceId']: ws['WorkspaceName'] for ws in workspaces_info}
        
        # Collect app info and app report tasks
        app_report_tasks = []
        for app in apps:
            app_workspace_id = app.get("workspaceId", "")
            
            if app_workspace_id in workspace_ids:
                app_id = app.get("id", "")
                app_name = app.get("name", "")
                app_workspace_name = workspace_name_lookup.get(app_workspace_id, "")
                
                apps_info.append({
                    "AppId": app_id,
                    "AppName": app_name,
                    "AppLastUpdate": app.get("lastUpdate", ""),
                    "AppDescription": app.get("description", ""),
                    "AppPublishedBy": app.get("publishedBy", ""),
                    "AppWorkspaceId": app_workspace_id,
                    "WorkspaceName": app_workspace_name
                })
                
                app_report_tasks.append((app_id, app_name, app_workspace_name))
        
        # Fetch app reports in parallel
        def fetch_app_reports(task):
            a_id, a_name, a_ws_name = task
            reports = []
            try:
                app_reports_url = f"v1.0/myorg/apps/{a_id}/reports"
                app_reports_response = client.get(app_reports_url)
                if app_reports_response.status_code == 200:
                    for report in app_reports_response.json().get('value', []):
                        reports.append({
                            "AppId": a_id,
                            "AppName": a_name,
                            "AppReportId": report.get("id", ""),
                            "AppReportType": report.get("reportType", ""),
                            "ReportName": report.get("name", ""),
                            "AppReportWebUrl": report.get("webUrl", ""),
                            "AppReportEmbedUrl": report.get("embedUrl", ""),
                            "AppReportIsOwnedByMe": str(bool(report.get("isOwnedByMe", False))),
                            "AppReportDatasetId": report.get("datasetId", ""),
                            "ReportId": report.get("originalReportObjectId", ""),
                            "WorkspaceName": a_ws_name
                        })
            except Exception as e:
                log(f"  ERROR fetching app reports for {a_name}: {e}")
            return reports
        
        if app_report_tasks:
            log(f"Fetching app reports in parallel (max {MAX_WORKERS} workers)...")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                for app_reports_result in executor.map(fetch_app_reports, app_report_tasks):
                    reports_in_app_info.extend(app_reports_result)
                    
    else:
        log(f"No apps found or unable to fetch apps")
        
except Exception as e:
    log(f"ERROR fetching apps: {e}")

# ==============================================================  
# DATAFLOW LINEAGE
# ==============================================================

log("\n" + "="*80)
log("Fetching Dataflow Lineage")
log("="*80)

for ws_info in workspaces_info:
    ws_name = ws_info["WorkspaceName"]
    ws_id = ws_info["WorkspaceId"]
    
    try:
        lineage_url = f"v1.0/myorg/groups/{ws_id}/dataflows/upstreamDataflows"
        response = client.get(lineage_url)
        
        if response.status_code == 200:
            lineage_items = response.json().get('value', [])
            
            for lineage in lineage_items:
                dataflow_id = lineage.get("dataflowObjectId", "")
                dataset_id = lineage.get("datasetObjectId", "")
                
                dataflow_lineage.append({
                    "WorkspaceId": ws_id,
                    "WorkspaceName": ws_name,
                    "DataflowId": dataflow_id,
                    "DataflowName": dataflow_name_lookup.get(dataflow_id, "Unknown Dataflow"),
                    "DatasetId": dataset_id,
                    "DatasetName": dataset_name_lookup.get(dataset_id, "Unknown Dataset")
                })
    except Exception as e:
        log(f"  Could not fetch dataflow lineage for {ws_name}: {e}")

log("✓ Dataflow lineage collection complete")

# ==============================================================  
# WRITE TO LAKEHOUSE
# ==============================================================

log("\n" + "="*80)
log("Writing output to Lakehouse")
log("="*80)

def write_table(data, name, sample_row=None):
    full_name = f"{CATALOG}.{LAKEHOUSE_SCHEMA}.{name}"
    
    if not data:
        # Create empty table using sample row structure if provided
        if sample_row:
            log(f"Creating empty table with schema: {name}")
            pandas_df = pd.DataFrame([sample_row])
            df = spark.createDataFrame(pandas_df)
            # Filter to create empty dataframe with schema
            empty_df = df.filter("1=0")
            empty_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(full_name)
            log(f"✓ Created empty table: {full_name}\n")
        else:
            log(f"⚠ Empty table skipped (no schema): {name}\n")
        return

    # Convert to pandas DataFrame first for proper type handling, then to Spark
    pandas_df = pd.DataFrame(data)
    df = spark.createDataFrame(pandas_df)

    log(f"Writing {len(data)} rows → {full_name}")

    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(full_name)

    log(f"✓ Wrote table: {full_name}\n")

# Write all tables matching PowerShell script worksheets
write_table(workspaces_info, "Workspaces", SAMPLE_ROWS.get("Workspaces"))
write_table(fabric_items_info, "FabricItems", SAMPLE_ROWS.get("FabricItems"))
write_table(datasets_info, "Datasets", SAMPLE_ROWS.get("Datasets"))
write_table(dataset_sources_info, "DatasetSourcesInfo", SAMPLE_ROWS.get("DatasetSourcesInfo"))
write_table(dataset_refresh_history, "DatasetRefreshHistory", SAMPLE_ROWS.get("DatasetRefreshHistory"))
write_table(dataset_refresh_schedule, "DatasetRefreshSchedule", SAMPLE_ROWS.get("DatasetRefreshSchedule"))
write_table(dataflows_info, "Dataflows", SAMPLE_ROWS.get("Dataflows"))
write_table(dataflow_lineage, "DataflowLineage", SAMPLE_ROWS.get("DataflowLineage"))
write_table(dataflow_sources_info, "DataflowSourcesInfo", SAMPLE_ROWS.get("DataflowSourcesInfo"))
write_table(dataflow_refresh_history, "DataflowRefreshHistory", SAMPLE_ROWS.get("DataflowRefreshHistory"))
write_table(reports_info, "Reports", SAMPLE_ROWS.get("Reports"))
write_table(report_pages_info, "ReportPages", SAMPLE_ROWS.get("ReportPages"))
write_table(apps_info, "Apps", SAMPLE_ROWS.get("Apps"))
write_table(reports_in_app_info, "AppReports", SAMPLE_ROWS.get("AppReports"))

# ==============================================================  
# END
# ==============================================================

heartbeat_running = False

log("\n" + "="*80)
log("PROCESS COMPLETE")
log(f"Finished at: {datetime.now()}")
log(f"Total runtime: {elapsed_min():.2f} minutes")
log("="*80)


# In[2]:


# ================================
# FABRIC MODEL METADATA EXTRACTOR (TOMWrapper)
# WITH AUTO-SCHEMA CREATION
# ================================

# %pip install semantic-link-labs --quiet

import time, re, pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import sempy.fabric as fabric
from sempy_labs.tom import TOMWrapper
from sempy_labs._model_dependencies import get_model_calc_dependencies

# Uses shared configuration from Cell 0: LAKEHOUSE_SCHEMA, WORKSPACE_NAMES, SCAN_ALL_WORKSPACES, MAX_PARALLEL_WORKERS

EXTRACTION_TIMESTAMP = datetime.now()
REPORT_DATE = EXTRACTION_TIMESTAMP.strftime("%Y-%m-%d")
start_time = time.time()

# -----------------------------------
# Logging helpers
# -----------------------------------
def log(msg):
    print(msg, flush=True)

def elapsed_min():
    return (time.time() - start_time) / 60

# Heartbeat
import threading
heartbeat_running = True
def heartbeat():
    while heartbeat_running:
        time.sleep(1000)
        print(f"[Heartbeat] Still running… elapsed {elapsed_min():.2f} min", flush=True)

threading.Thread(target=heartbeat, daemon=True).start()

# -----------------------------------
# Start banner
# -----------------------------------
log("="*80)
log("FABRIC MODEL METADATA EXTRACTION")
log(f"Started: {EXTRACTION_TIMESTAMP}")
log("="*80)

# ============================================
# AUTO-CREATE SCHEMA (LAKEHOUSE)
# ============================================
CATALOG = spark.sql("SELECT current_catalog()").first()[0]
log(f"Using catalog: {CATALOG}")

schema_name = f"{CATALOG}.{LAKEHOUSE_SCHEMA}"
log(f"Ensuring lakehouse schema exists: {schema_name}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
log(f"✓ Schema is ready: {schema_name}\n")

# ==============================================================  


# ==============================================================  
# COLLECTIONS & SCHEMA TEMPLATES
# ==============================================================
# Each collection includes a template row that defines the schema.
# This ensures empty tables can be created with correct column structure.

all_model_details = [{
    "Type": "",
    "Table": "",
    "Name": "",
    "FormatString": "",
    "DisplayFolder": "",
    "Description": "",
    "IsHidden": "",
    "TableStorageMode": "",
    "Expression": "",
    "ModelAsOfDate": "",
    "ModelName": "",
    "ModelID": "",
    "WorkspaceName": "",
    "RelationshipFromTable": "",
    "RelationshipFromColumn": "",
    "RelationshipToTable": "",
    "RelationshipToColumn": "",
    "RelationshipStatus": "",
    "RelationshipFromCardinality": "",
    "RelationshipToCardinality": "",
    "RelationshipCrossFilteringBehavior": ""
}]

# Schema template for model dependencies
# Based on the Measure Dependency Extract Script.csx from:
# https://github.com/chris1642/Power-BI-Backup-Impact-Analysis-Governance-Solution
all_model_dependencies = [{
    "ObjectName": "",
    "ObjectType": "",
    "DependsOn": "",
    "DependsOnType": "",
    "ModelAsOfDate": "",
    "ModelName": "",
    "ModelID": "",
    "WorkspaceName": ""
}]

# ==============================================================  
# HELPER FUNCTIONS
# ==============================================================

def format_dax_object_name(table_name, object_name):
    """Format a DAX object name as 'TableName'[ObjectName]"""
    return f"'{table_name}'[{object_name}]"

def get_dependency_name(dep_obj):
    """
    Get the formatted name of a dependency object based on its type.
    
    Args:
        dep_obj: The TOM object representing the dependency
    
    Returns:
        str: The formatted dependency name
    """
    dep_type = str(dep_obj.ObjectType)
    
    if dep_type in ["Measure", "Column"]:
        return format_dax_object_name(dep_obj.Parent.Name, dep_obj.Name)
    elif dep_type == "Table":
        return f"'{dep_obj.Name}'"
    else:
        return dep_obj.Name

def get_friendly_error_message(error, context=""):
    """
    Parse error messages and return user-friendly descriptions.
    
    Args:
        error: The exception or error message string
        context: Optional context for the error message (e.g., "opening model", "accessing workspace")
    
    Returns:
        str: User-friendly error message
    """
    error_msg = str(error)
    context_suffix = f" {context}" if context else ""
    
    # Check for common error patterns
    if "does not have permission" in error_msg or "Discover method" in error_msg:
        return f"Insufficient permissions{context_suffix}"
    elif "session" in error_msg.lower() and ("timeout" in error_msg.lower() or "expired" in error_msg.lower() or "cannot be found" in error_msg.lower()):
        return f"Session timeout or connection lost{context_suffix}"
    elif "database is empty" in error_msg.lower():
        return "Database is empty (staging lakehouse or no data)"
    elif "'NoneType' object has no attribute" in error_msg:
        return "Model connection may have been lost"
    else:
        return str(error)

# ==============================================================  
# GET WORKSPACES
# ==============================================================

workspaces_df = fabric.list_workspaces()

if not SCAN_ALL_WORKSPACES:
    workspaces_df = workspaces_df[workspaces_df["Name"].isin(WORKSPACE_NAMES)]
    if workspaces_df.empty:
        raise ValueError(f"No workspaces found matching: {WORKSPACE_NAMES}")
    log(f"Filtering to workspaces: {WORKSPACE_NAMES}")

log(f"Workspace count: {len(workspaces_df)}")
log("")

# ==============================================================  
# MODEL METADATA EXTRACTION (PARALLELIZED)
# ==============================================================

def extract_single_model(ws_name, model_name, model_id, report_date):
    """Extract all metadata for a single model. Returns (details, dependencies, error_msg).
    Includes retry logic for session timeout errors on dependency extraction."""
    details = []
    dependencies = []
    log_msgs = []

    t0 = time.time()

    try:
        tom = TOMWrapper(dataset=model_name, workspace=ws_name, readonly=True)
    except Exception as e:
        return details, dependencies, f"ERROR opening model {model_name}: {get_friendly_error_message(e)}"

    measures = []
    calc_columns = []
    calc_items = []

    # -------------------- Tables --------------------
    try:
        tables = tom.model.Tables
        log_msgs.append(f"    Tables: {len(tables)}")
        for t in tables:
            storage_mode = ""
            if t.Partitions.Count > 0:
                for p in t.Partitions:
                    if hasattr(p, 'Mode'):
                        storage_mode = p.Mode.ToString()
                    break
            details.append({
                "Type": "Table", "Table": t.Name, "Name": t.Name,
                "FormatString": "", "DisplayFolder": "", "Description": "",
                "IsHidden": str(t.IsHidden), "TableStorageMode": storage_mode,
                "Expression": "", "ModelAsOfDate": report_date,
                "ModelName": model_name, "ModelID": model_id, "WorkspaceName": ws_name,
                "RelationshipFromTable": "", "RelationshipFromColumn": "",
                "RelationshipToTable": "", "RelationshipToColumn": "",
                "RelationshipStatus": "", "RelationshipFromCardinality": "",
                "RelationshipToCardinality": "", "RelationshipCrossFilteringBehavior": ""
            })
    except Exception as e:
        log_msgs.append(f"    ERROR extracting Tables: {e}")

    # -------------------- Calculation Groups --------------------
    try:
        calc_groups = list(tom.all_calculation_groups())
        log_msgs.append(f"    Calculation Groups: {len(calc_groups)}")
        for cg in calc_groups:
            details.append({
                "Type": "CalculationGroup", "Table": cg.Name, "Name": cg.Name,
                "FormatString": "", "DisplayFolder": "",
                "Description": cg.Description if cg.Description else "",
                "IsHidden": str(cg.IsHidden), "TableStorageMode": "",
                "Expression": "", "ModelAsOfDate": report_date,
                "ModelName": model_name, "ModelID": model_id, "WorkspaceName": ws_name,
                "RelationshipFromTable": "", "RelationshipFromColumn": "",
                "RelationshipToTable": "", "RelationshipToColumn": "",
                "RelationshipStatus": "", "RelationshipFromCardinality": "",
                "RelationshipToCardinality": "", "RelationshipCrossFilteringBehavior": ""
            })
    except Exception as e:
        log_msgs.append(f"    ERROR extracting Calculation Groups: {e}")

    # -------------------- Calculation Items --------------------
    try:
        extracted_calc_items = list(tom.all_calculation_items())
        log_msgs.append(f"    Calculation Items: {len(extracted_calc_items)}")
        for ci in extracted_calc_items:
            parent_table_name = ""
            try:
                if hasattr(ci, 'Parent') and ci.Parent and hasattr(ci.Parent, 'Name'):
                    parent_table_name = ci.Parent.Name
                elif hasattr(ci, 'CalculationGroup') and ci.CalculationGroup and hasattr(ci.CalculationGroup, 'Name'):
                    parent_table_name = ci.CalculationGroup.Name
            except Exception:
                parent_table_name = "Unknown"
            details.append({
                "Type": "CalculationItem", "Table": parent_table_name, "Name": ci.Name,
                "FormatString": "", "DisplayFolder": "",
                "Description": ci.Description if ci.Description else "",
                "IsHidden": "", "TableStorageMode": "",
                "Expression": ci.Expression if ci.Expression else "",
                "ModelAsOfDate": report_date,
                "ModelName": model_name, "ModelID": model_id, "WorkspaceName": ws_name,
                "RelationshipFromTable": "", "RelationshipFromColumn": "",
                "RelationshipToTable": "", "RelationshipToColumn": "",
                "RelationshipStatus": "", "RelationshipFromCardinality": "",
                "RelationshipToCardinality": "", "RelationshipCrossFilteringBehavior": ""
            })
        calc_items = extracted_calc_items
    except Exception as e:
        log_msgs.append(f"    ERROR extracting Calculation Items: {e}")

    # -------------------- Columns --------------------
    try:
        columns = list(tom.all_columns())
        log_msgs.append(f"    Columns: {len(columns)}")
        for col in columns:
            details.append({
                "Type": "Column", "Table": col.Table.Name, "Name": col.Name,
                "FormatString": col.FormatString if col.FormatString else "",
                "DisplayFolder": col.DisplayFolder if col.DisplayFolder else "",
                "Description": col.Description if col.Description else "",
                "IsHidden": str(col.IsHidden), "TableStorageMode": "",
                "Expression": "", "ModelAsOfDate": report_date,
                "ModelName": model_name, "ModelID": model_id, "WorkspaceName": ws_name,
                "RelationshipFromTable": "", "RelationshipFromColumn": "",
                "RelationshipToTable": "", "RelationshipToColumn": "",
                "RelationshipStatus": "", "RelationshipFromCardinality": "",
                "RelationshipToCardinality": "", "RelationshipCrossFilteringBehavior": ""
            })
    except Exception as e:
        log_msgs.append(f"    ERROR extracting Columns: {e}")

    # -------------------- Calculated Columns --------------------
    try:
        extracted_calc_columns = list(tom.all_calculated_columns())
        log_msgs.append(f"    Calculated Columns: {len(extracted_calc_columns)}")
        for col in extracted_calc_columns:
            details.append({
                "Type": "CalculatedColumn", "Table": col.Table.Name, "Name": col.Name,
                "FormatString": col.FormatString if col.FormatString else "",
                "DisplayFolder": col.DisplayFolder if col.DisplayFolder else "",
                "Description": col.Description if col.Description else "",
                "IsHidden": str(col.IsHidden), "TableStorageMode": "",
                "Expression": col.Expression if col.Expression else "",
                "ModelAsOfDate": report_date,
                "ModelName": model_name, "ModelID": model_id, "WorkspaceName": ws_name,
                "RelationshipFromTable": "", "RelationshipFromColumn": "",
                "RelationshipToTable": "", "RelationshipToColumn": "",
                "RelationshipStatus": "", "RelationshipFromCardinality": "",
                "RelationshipToCardinality": "", "RelationshipCrossFilteringBehavior": ""
            })
        calc_columns = extracted_calc_columns
    except Exception as e:
        log_msgs.append(f"    ERROR extracting Calculated Columns: {e}")

    # -------------------- Measures --------------------
    try:
        extracted_measures = list(tom.all_measures())
        log_msgs.append(f"    Measures: {len(extracted_measures)}")
        for m in extracted_measures:
            details.append({
                "Type": "Measure", "Table": m.Table.Name, "Name": m.Name,
                "FormatString": m.FormatString if m.FormatString else "",
                "DisplayFolder": m.DisplayFolder if m.DisplayFolder else "",
                "Description": m.Description if m.Description else "",
                "IsHidden": str(m.IsHidden), "TableStorageMode": "",
                "Expression": m.Expression if m.Expression else "",
                "ModelAsOfDate": report_date,
                "ModelName": model_name, "ModelID": model_id, "WorkspaceName": ws_name,
                "RelationshipFromTable": "", "RelationshipFromColumn": "",
                "RelationshipToTable": "", "RelationshipToColumn": "",
                "RelationshipStatus": "", "RelationshipFromCardinality": "",
                "RelationshipToCardinality": "", "RelationshipCrossFilteringBehavior": ""
            })
        measures = extracted_measures
    except Exception as e:
        log_msgs.append(f"    ERROR extracting Measures: {e}")

    # -------------------- Hierarchies --------------------
    try:
        hierarchies = list(tom.all_hierarchies())
        log_msgs.append(f"    Hierarchies: {len(hierarchies)}")
        for h in hierarchies:
            details.append({
                "Type": "Hierarchy", "Table": h.Table.Name, "Name": h.Name,
                "FormatString": "", "DisplayFolder": h.DisplayFolder if h.DisplayFolder else "",
                "Description": h.Description if h.Description else "",
                "IsHidden": str(h.IsHidden), "TableStorageMode": "",
                "Expression": "", "ModelAsOfDate": report_date,
                "ModelName": model_name, "ModelID": model_id, "WorkspaceName": ws_name,
                "RelationshipFromTable": "", "RelationshipFromColumn": "",
                "RelationshipToTable": "", "RelationshipToColumn": "",
                "RelationshipStatus": "", "RelationshipFromCardinality": "",
                "RelationshipToCardinality": "", "RelationshipCrossFilteringBehavior": ""
            })
    except Exception as e:
        log_msgs.append(f"    ERROR extracting Hierarchies: {e}")

    # -------------------- Levels --------------------
    try:
        levels = list(tom.all_levels())
        log_msgs.append(f"    Levels: {len(levels)}")
        for l in levels:
            details.append({
                "Type": "Level", "Table": l.Hierarchy.Table.Name, "Name": l.Name,
                "FormatString": "", "DisplayFolder": "",
                "Description": l.Description if l.Description else "",
                "IsHidden": "", "TableStorageMode": "",
                "Expression": "", "ModelAsOfDate": report_date,
                "ModelName": model_name, "ModelID": model_id, "WorkspaceName": ws_name,
                "RelationshipFromTable": "", "RelationshipFromColumn": "",
                "RelationshipToTable": "", "RelationshipToColumn": "",
                "RelationshipStatus": "", "RelationshipFromCardinality": "",
                "RelationshipToCardinality": "", "RelationshipCrossFilteringBehavior": ""
            })
    except Exception as e:
        log_msgs.append(f"    ERROR extracting Levels: {e}")

    # -------------------- Partitions --------------------
    try:
        partitions = list(tom.all_partitions())
        log_msgs.append(f"    Partitions: {len(partitions)}")
        for p in partitions:
            storage_mode = p.Mode.ToString() if hasattr(p, 'Mode') else ""
            expression = ""
            if hasattr(p, 'Source') and p.Source:
                if hasattr(p.Source, 'Expression'):
                    expression = p.Source.Expression if p.Source.Expression else ""
            details.append({
                "Type": "Partition", "Table": p.Table.Name, "Name": p.Name,
                "FormatString": "", "DisplayFolder": "",
                "Description": p.Description if p.Description else "",
                "IsHidden": "", "TableStorageMode": storage_mode,
                "Expression": expression, "ModelAsOfDate": report_date,
                "ModelName": model_name, "ModelID": model_id, "WorkspaceName": ws_name,
                "RelationshipFromTable": "", "RelationshipFromColumn": "",
                "RelationshipToTable": "", "RelationshipToColumn": "",
                "RelationshipStatus": "", "RelationshipFromCardinality": "",
                "RelationshipToCardinality": "", "RelationshipCrossFilteringBehavior": ""
            })
    except Exception as e:
        log_msgs.append(f"    ERROR extracting Partitions: {e}")

    # -------------------- Relationships --------------------
    try:
        relationships = tom.model.Relationships
        log_msgs.append(f"    Relationships: {len(relationships)}")
        for r in relationships:
            details.append({
                "Type": "Relationship", "Table": r.FromTable.Name, "Name": r.FromColumn.Name,
                "FormatString": "", "DisplayFolder": "", "Description": "",
                "IsHidden": "", "TableStorageMode": "",
                "Expression": r.Name if r.Name else "",
                "ModelAsOfDate": report_date,
                "ModelName": model_name, "ModelID": model_id, "WorkspaceName": ws_name,
                "RelationshipFromTable": r.FromTable.Name,
                "RelationshipFromColumn": r.FromColumn.Name,
                "RelationshipToTable": r.ToTable.Name,
                "RelationshipToColumn": r.ToColumn.Name,
                "RelationshipStatus": str(r.IsActive),
                "RelationshipFromCardinality": r.FromCardinality.ToString(),
                "RelationshipToCardinality": r.ToCardinality.ToString(),
                "RelationshipCrossFilteringBehavior": r.CrossFilteringBehavior.ToString()
            })
    except Exception as e:
        log_msgs.append(f"    ERROR extracting Relationships: {e}")

    # -------------------- Model Dependencies (with retry for session timeouts) --------------------
    if not EXTRACT_MODEL_DEPENDENCIES:
        log_msgs.append(f"    Skipping dependencies (EXTRACT_MODEL_DEPENDENCIES=False)")
    else:
      MAX_DEP_RETRIES = 2
      for dep_attempt in range(MAX_DEP_RETRIES + 1):
        try:
            # On retry, reconnect TOMWrapper with fresh auth
            if dep_attempt > 0:
                log_msgs.append(f"    Retrying dependencies (attempt {dep_attempt + 1}/{MAX_DEP_RETRIES + 1})...")
                try:
                    tom = TOMWrapper(dataset=model_name, workspace=ws_name, readonly=True)
                    # Re-extract measures/calc_columns/calc_items from fresh connection
                    measures = list(tom.all_measures())
                    calc_columns = list(tom.all_calculated_columns())
                    calc_items = list(tom.all_calculation_items())
                except Exception as reconnect_e:
                    log_msgs.append(f"    Could not reconnect for retry: {get_friendly_error_message(reconnect_e)}")
                    break

            has_tables = (hasattr(tom.model, 'Tables') and 
                         hasattr(tom.model.Tables, 'Count') and 
                         tom.model.Tables.Count > 0)
            
            if not has_tables:
                log_msgs.append(f"    Warning: Skipping dependencies - model has no tables")
                break
            elif not measures and not calc_columns and not calc_items:
                log_msgs.append(f"    Warning: Skipping dependencies - no calculated objects to analyze")
                break
            else:
                dependencies_df = get_model_calc_dependencies(
                    dataset=model_name,
                    workspace=ws_name
                )
                
                if dependencies_df is not None and not dependencies_df.empty:
                    dep_count = 0
                    
                    for m in measures:
                        try:
                            for dep_obj in tom.depends_on(object=m, dependencies=dependencies_df):
                                dependencies.append({
                                    "ObjectName": m.Name, "ObjectType": "Measure",
                                    "DependsOn": get_dependency_name(dep_obj),
                                    "DependsOnType": str(dep_obj.ObjectType),
                                    "ModelAsOfDate": report_date,
                                    "ModelName": model_name, "ModelID": model_id,
                                    "WorkspaceName": ws_name
                                })
                                dep_count += 1
                        except Exception as e:
                            log_msgs.append(f"      Warning: Could not get dependencies for measure {m.Name}: {e}")

                    for col in calc_columns:
                        try:
                            for dep_obj in tom.depends_on(object=col, dependencies=dependencies_df):
                                dependencies.append({
                                    "ObjectName": col.Name, "ObjectType": "CalculatedColumn",
                                    "DependsOn": get_dependency_name(dep_obj),
                                    "DependsOnType": str(dep_obj.ObjectType),
                                    "ModelAsOfDate": report_date,
                                    "ModelName": model_name, "ModelID": model_id,
                                    "WorkspaceName": ws_name
                                })
                                dep_count += 1
                        except Exception as e:
                            log_msgs.append(f"      Warning: Could not get dependencies for calculated column {col.Name}: {e}")

                    for ci in calc_items:
                        try:
                            for dep_obj in tom.depends_on(object=ci, dependencies=dependencies_df):
                                dependencies.append({
                                    "ObjectName": ci.Name, "ObjectType": "CalculationItem",
                                    "DependsOn": get_dependency_name(dep_obj),
                                    "DependsOnType": str(dep_obj.ObjectType),
                                    "ModelAsOfDate": report_date,
                                    "ModelName": model_name, "ModelID": model_id,
                                    "WorkspaceName": ws_name
                                })
                                dep_count += 1
                        except Exception as e:
                            log_msgs.append(f"      Warning: Could not get dependencies for calculation item {ci.Name}: {e}")
                    
                    log_msgs.append(f"    Dependencies extracted: {dep_count}")
                else:
                    log_msgs.append(f"    No dependencies found")
                break  # Success, no retry needed
        except Exception as e:
            error_msg = str(e).lower()
            is_session_error = ("session" in error_msg and ("timeout" in error_msg or "expired" in error_msg or "cannot be found" in error_msg)) or \
                               "does not have permission" in error_msg or \
                               "'NoneType' object has no attribute" in error_msg
            
            if is_session_error and dep_attempt < MAX_DEP_RETRIES:
                log_msgs.append(f"    Session error during dependencies: {get_friendly_error_message(e)} → will retry")
                dependencies = []  # Clear partial results before retry
                continue
            else:
                log_msgs.append(f"    Warning: Could not extract dependencies - {get_friendly_error_message(e)}")
                break

    elapsed_sec = time.time() - t0
    log_msgs.append(f"  → Finished {model_name} in {elapsed_sec:.1f} sec")

    # Print all accumulated log messages at once (thread-safe batch)
    for msg in log_msgs:
        log(msg)

    return details, dependencies, None


# Collect all model tasks across workspaces
model_tasks = []
for ws_row in workspaces_df.itertuples(index=False):
    ws_name = ws_row.Name
    log(f"\nScanning workspace: {ws_name} | Elapsed: {elapsed_min():.2f} min")

    try:
        datasets_df = fabric.list_datasets(workspace=ws_name)
        if datasets_df is None or datasets_df.empty:
            log("  No datasets found.")
            continue

        log(f"  Datasets found: {len(datasets_df)}")

        for idx, row in datasets_df.iterrows():
            model_name = row.get('Dataset Name') or row.get('Name') or row.get('Display Name', '')
            model_id = row.get('Dataset ID') or row.get('Id') or row.get('ID', '')
            model_tasks.append((ws_name, model_name, model_id))

    except Exception as e:
        log(f"ERROR accessing workspace {ws_name}: {get_friendly_error_message(e, 'accessing workspace')}")

# Process all models in parallel
log(f"\nExtracting {len(model_tasks)} models in parallel (max {MAX_PARALLEL_WORKERS} workers)...")
with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
    futures = {
        executor.submit(extract_single_model, ws, mn, mi, REPORT_DATE): (ws, mn)
        for ws, mn, mi in model_tasks
    }
    completed = 0
    for future in as_completed(futures):
        completed += 1
        ws, mn = futures[future]
        try:
            details, deps, error = future.result()
            if error:
                log(f"  [{completed}/{len(model_tasks)}] {error}")
            else:
                all_model_details.extend(details)
                all_model_dependencies.extend(deps)
                log(f"  [{completed}/{len(model_tasks)}] ✓ {mn} ({len(details)} details, {len(deps)} deps)")
        except Exception as e:
            log(f"  [{completed}/{len(model_tasks)}] ERROR {mn}: {e}")

# ==============================================================  
# WRITE TO LAKEHOUSE
# ==============================================================

log("\n" + "="*80)
log("Writing output to Lakehouse")
log("="*80)

def write_table(data, name):
    """
    Write data to a Delta table. Schema is inferred from the first row (template).
    Creates empty table with schema if only template row exists.
    
    Args:
        data: List of dictionaries containing the data (first row is schema template)
        name: Name of the table
    """
    full_name = f"{CATALOG}.{LAKEHOUSE_SCHEMA}.{name}"
    
    # Check if we only have the template row (length 1 means just the schema template)
    if len(data) == 1:
        log(f"⚠ No data for {name}, creating empty table with schema")
        # Use template to create empty DataFrame with correct schema
        df = spark.createDataFrame(pd.DataFrame(data))
        # Filter out the template row to create truly empty table
        empty_df = df.filter("1=0")
        empty_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(full_name)
        log(f"✓ Created empty table: {full_name}\n")
        return

    # Skip the template row (first row) and create DataFrame with actual data
    pandas_df = pd.DataFrame(data)
    actual_df = spark.createDataFrame(pandas_df.iloc[1:])

    log(f"Writing {len(data) - 1} rows → {full_name}")

    actual_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(full_name)

    log(f"✓ Wrote table: {full_name}\n")

write_table(all_model_details, "ModelDetail")
write_table(all_model_dependencies, "ModelDependencies")

# ==============================================================  
# END
# ==============================================================

heartbeat_running = False

log("\n" + "="*80)
log("PROCESS COMPLETE")
log(f"Finished at: {datetime.now()}")
log(f"Total runtime: {elapsed_min():.2f} minutes")
log("="*80)

# In[3]:


# ================================
# FABRIC REPORT METADATA EXTRACTOR (ReportWrapper Only)
# WITH AUTO-SCHEMA CREATION
# ================================

# %pip install semantic-link-labs --quiet

import time, re, pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import sempy.fabric as fabric
from sempy_labs.report import ReportWrapper
# Note: Using private module for resolve_dataset_from_report - consider this dependency if upgrading semantic-link-labs
from sempy_labs._helper_functions import resolve_dataset_from_report

# Uses shared configuration from Cell 0: LAKEHOUSE_SCHEMA, WORKSPACE_NAMES, SCAN_ALL_WORKSPACES, MAX_PARALLEL_WORKERS

EXTRACTION_TIMESTAMP = datetime.now()
REPORT_DATE = EXTRACTION_TIMESTAMP.strftime("%Y-%m-%d")
start_time = time.time()

# -----------------------------------
# Logging helpers
# -----------------------------------
def log(msg):
    print(msg, flush=True)

def elapsed_min():
    return (time.time() - start_time) / 60

# Heartbeat
import threading
heartbeat_running = True
def heartbeat():
    while heartbeat_running:
        time.sleep(1000)
        print(f"[Heartbeat] Still running… elapsed {elapsed_min():.2f} min", flush=True)

threading.Thread(target=heartbeat, daemon=True).start()

# -----------------------------------
# Start banner
# -----------------------------------
log("="*80)
log("FABRIC REPORT METADATA EXTRACTION")
log(f"Started: {EXTRACTION_TIMESTAMP}")
log("="*80)

# ============================================
# AUTO-CREATE SCHEMA (LAKEHOUSE)
# ============================================
CATALOG = spark.sql("SELECT current_catalog()").first()[0]
log(f"Using catalog: {CATALOG}")

schema_name = f"{CATALOG}.{LAKEHOUSE_SCHEMA}"
log(f"Ensuring lakehouse schema exists: {schema_name}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
log(f"✓ Schema is ready: {schema_name}\n")



# ==============================================================  
# COLLECTIONS & SCHEMA TEMPLATES
# ==============================================================
# Each collection includes a template row that defines the schema.
# This ensures empty tables can be created with correct column structure.

all_connections = [{"ReportID": "", "ModelID": "", "ReportDate": "", "ReportName": "", "Type": "", "ServerName": "", "WorkspaceName": ""}]
all_pages = [{"ReportName": "", "ReportID": "", "ModelID": "", "Id": "", "Name": "", "Number": 0, "Width": 0, "Height": 0, "HiddenFlag": "", "VisualCount": 0, "Type": "", "DisplayOption": "", "DataVisualCount": 0, "VisibleVisualCount": 0, "PageFilterCount": 0, "ReportDate": "", "WorkspaceName": ""}]
all_visuals = [{"ReportName": "", "ReportID": "", "ModelID": "", "PageName": "", "PageId": "", "Id": "", "Name": "", "Type": "", "DisplayType": "", "Title": "", "SubTitle": "", "AltText": "", "TabOrder": 0, "CustomVisualFlag": "", "HiddenFlag": "", "X": 0.0, "Y": 0.0, "Z": 0, "Width": 0.0, "Height": 0.0, "ObjectCount": 0, "VisualFilterCount": 0, "DataLimit": 0, "Divider": "", "RowSubTotals": "", "ColumnSubTotals": "", "DataVisual": "", "HasSparkline": "", "ParentGroup": "", "ReportDate": "", "WorkspaceName": ""}]
all_bookmarks = [{"ReportName": "", "ReportID": "", "ModelID": "", "Name": "", "Id": "", "PageName": "", "PageId": "", "VisualId": "", "VisualHiddenFlag": "", "SuppressData": "", "CurrentPageSelected": "", "ApplyVisualDisplayState": "", "ApplyToAllVisuals": "", "ReportDate": "", "WorkspaceName": ""}]
all_custom_visuals = [{"ReportName": "", "ReportID": "", "ModelID": "", "Name": "", "ReportDate": "", "WorkspaceName": ""}]
all_report_filters = [{"ReportName": "", "ReportID": "", "ModelID": "", "displayName": "", "TableName": "", "ObjectName": "", "ObjectType": "", "FilterType": "", "HiddenFilter": "", "LockedFilter": "", "HowCreated": "", "Used": "", "ReportDate": "", "WorkspaceName": ""}]
all_page_filters = [{"ReportName": "", "ReportID": "", "ModelID": "", "PageId": "", "PageName": "", "displayName": "", "TableName": "", "ObjectName": "", "ObjectType": "", "FilterType": "", "HiddenFilter": "", "LockedFilter": "", "HowCreated": "", "Used": "", "ReportDate": "", "WorkspaceName": ""}]
all_visual_filters = [{"ReportName": "", "ReportID": "", "ModelID": "", "PageName": "", "PageId": "", "VisualId": "", "TableName": "", "ObjectName": "", "ObjectType": "", "FilterType": "", "HiddenFilter": "", "LockedFilter": "", "displayName": "", "HowCreated": "", "Used": "", "ReportDate": "", "WorkspaceName": ""}]
all_visual_objects = [{"ReportName": "", "ReportID": "", "ModelID": "", "PageName": "", "PageId": "", "VisualId": "", "VisualName": "", "VisualType": "", "CustomVisualFlag": "", "TableName": "", "ObjectName": "", "ObjectType": "", "Source": "", "displayName": "", "ImplicitMeasure": "", "Sparkline": "", "VisualCalc": "", "Format": "", "ReportDate": "", "WorkspaceName": ""}]
all_report_level_measures = [{"ReportName": "", "ReportID": "", "ModelID": "", "TableName": "", "ObjectName": "", "ObjectType": "", "Expression": "", "HiddenFlag": "", "FormatString": "", "DataType": "", "DataCategory": "", "ReportDate": "", "WorkspaceName": ""}]
all_visual_interactions = [{"ReportName": "", "ReportID": "", "ModelID": "", "PageName": "", "PageId": "", "SourceVisualID": "", "TargetVisualID": "", "SourceVisualName": "", "TargetVisualName": "", "TypeID": "", "Type": "", "ReportDate": "", "WorkspaceName": ""}]

# ==============================================================  
# PARALLEL REPORT EXTRACTION HELPER
# ==============================================================

def extract_report_metadata(ws_name, rpt_name, rpt_id, model_id, report_date):
    """Extract metadata for a single report using ReportWrapper"""
    result = {
        'connections': [],
        'pages': [],
        'visuals': [],
        'bookmarks': [],
        'custom_visuals': [],
        'report_filters': [],
        'page_filters': [],
        'visual_filters': [],
        'visual_objects': [],
        'report_level_measures': [],
        'visual_interactions': [],
        'error': None
    }
    
    try:
        rpt = ReportWrapper(report=rpt_name, workspace=ws_name)
        
        # Add connection record
        result['connections'].append({
            "ReportID": rpt_id,
            "ModelID": model_id,
            "ReportDate": report_date,
            "ReportName": rpt_name,
            "Type": "",
            "ServerName": "",
            "WorkspaceName": ws_name
        })
        
        # Pages
        df = rpt.list_pages()
        if isinstance(df, pd.DataFrame) and not df.empty:
            for _, row in df.iterrows():
                result['pages'].append({
                    "ReportName": rpt_name,
                    "ReportID": rpt_id,
                    "ModelID": model_id,
                    "Id": row.get("Page Name", ""),
                    "Name": row.get("Page Display Name", ""),
                    "Number": 0,
                    "Width": row.get("Width", 0),
                    "Height": row.get("Height", 0),
                    "HiddenFlag": str(bool(row.get("Hidden", False))),
                    "VisualCount": row.get("Visual Count", 0),
                    "Type": row.get("Display Option", ""),
                    "DisplayOption": row.get("Display Option", ""),
                    "DataVisualCount": row.get("Data Visual Count", 0),
                    "VisibleVisualCount": row.get("Visible Visual Count", 0),
                    "PageFilterCount": row.get("Page Filter Count", 0),
                    "ReportDate": report_date,
                    "WorkspaceName": ws_name
                })
        
        # Visuals
        df = rpt.list_visuals()
        if isinstance(df, pd.DataFrame) and not df.empty:
            for _, row in df.iterrows():
                result['visuals'].append({
                    "ReportName": rpt_name,
                    "ReportID": rpt_id,
                    "ModelID": model_id,
                    "PageName": row.get("Page Display Name", ""),
                    "PageId": row.get("Page Name", ""),
                    "Id": row.get("Visual Name", ""),
                    "Name": row.get("Visual Name", ""),
                    "Type": row.get("Type", ""),
                    "DisplayType": row.get("Display Type", ""),
                    "Title": row.get("Title", ""),
                    "SubTitle": row.get("Sub Title", ""),
                    "AltText": row.get("Alt Text", ""),
                    "TabOrder": row.get("Tab Order", 0),
                    "CustomVisualFlag": str(bool(row.get("Custom Visual", False))),
                    "HiddenFlag": str(bool(row.get("Hidden", False))),
                    "X": row.get("X", 0),
                    "Y": row.get("Y", 0),
                    "Z": row.get("Z", 0),
                    "Width": row.get("Width", 0),
                    "Height": row.get("Height", 0),
                    "ObjectCount": row.get("Visual Object Count", 0),
                    "VisualFilterCount": row.get("Visual Filter Count", 0),
                    "DataLimit": row.get("Data Limit", 0),
                    "Divider": str(bool(row.get("Divider", False))),
                    "RowSubTotals": str(bool(row.get("Row Sub Totals", False))),
                    "ColumnSubTotals": str(bool(row.get("Column Sub Totals", False))),
                    "DataVisual": str(bool(row.get("Data Visual", False))),
                    "HasSparkline": str(bool(row.get("Has Sparkline", False))),
                    "ParentGroup": "",
                    "ReportDate": report_date,
                    "WorkspaceName": ws_name
                })
        
        # Bookmarks
        df = rpt.list_bookmarks()
        if isinstance(df, pd.DataFrame) and not df.empty:
            for _, row in df.iterrows():
                result['bookmarks'].append({
                    "ReportName": rpt_name,
                    "ReportID": rpt_id,
                    "ModelID": model_id,
                    "Name": row.get("Bookmark Display Name", ""),
                    "Id": row.get("Bookmark Name", ""),
                    "PageName": row.get("Page Display Name", ""),
                    "PageId": row.get("Page Name", ""),
                    "VisualId": row.get("Visual Name", ""),
                    "VisualHiddenFlag": str(bool(row.get("Visual Hidden", False))),
                    "SuppressData": str(bool(row.get("Suppress Data", False))),
                    "CurrentPageSelected": str(bool(row.get("Current Page Selected", False))),
                    "ApplyVisualDisplayState": str(bool(row.get("Apply Visual Display State", False))),
                    "ApplyToAllVisuals": str(bool(row.get("Apply To All Visuals", False))),
                    "ReportDate": report_date,
                    "WorkspaceName": ws_name
                })
        
        # Custom Visuals
        df = rpt.list_custom_visuals()
        if isinstance(df, pd.DataFrame) and not df.empty:
            for _, row in df.iterrows():
                result['custom_visuals'].append({
                    "ReportName": rpt_name,
                    "ReportID": rpt_id,
                    "ModelID": model_id,
                    "Name": row.get("Custom Visual Display Name", ""),
                    "ReportDate": report_date,
                    "WorkspaceName": ws_name
                })
        
        # Report Filters
        df = rpt.list_report_filters()
        if isinstance(df, pd.DataFrame) and not df.empty:
            for _, row in df.iterrows():
                result['report_filters'].append({
                    "ReportName": rpt_name,
                    "ReportID": rpt_id,
                    "ModelID": model_id,
                    "displayName": row.get("Filter Name", ""),
                    "TableName": row.get("Table Name", ""),
                    "ObjectName": row.get("Object Name", ""),
                    "ObjectType": row.get("Object Type", ""),
                    "FilterType": row.get("Type", ""),
                    "HiddenFilter": str(bool(row.get("Hidden", False))),
                    "LockedFilter": str(bool(row.get("Locked", False))),
                    "HowCreated": row.get("How Created", ""),
                    "Used": str(bool(row.get("Used", False))),
                    "ReportDate": report_date,
                    "WorkspaceName": ws_name
                })
        
        # Page Filters
        df = rpt.list_page_filters()
        if isinstance(df, pd.DataFrame) and not df.empty:
            for _, row in df.iterrows():
                result['page_filters'].append({
                    "ReportName": rpt_name,
                    "ReportID": rpt_id,
                    "ModelID": model_id,
                    "PageId": row.get("Page Name", ""),
                    "PageName": row.get("Page Display Name", ""),
                    "displayName": row.get("Filter Name", ""),
                    "TableName": row.get("Table Name", ""),
                    "ObjectName": row.get("Object Name", ""),
                    "ObjectType": row.get("Object Type", ""),
                    "FilterType": row.get("Type", ""),
                    "HiddenFilter": str(bool(row.get("Hidden", False))),
                    "LockedFilter": str(bool(row.get("Locked", False))),
                    "HowCreated": row.get("How Created", ""),
                    "Used": str(bool(row.get("Used", False))),
                    "ReportDate": report_date,
                    "WorkspaceName": ws_name
                })
        
        # Visual Filters
        df = rpt.list_visual_filters()
        if isinstance(df, pd.DataFrame) and not df.empty:
            for _, row in df.iterrows():
                result['visual_filters'].append({
                    "ReportName": rpt_name,
                    "ReportID": rpt_id,
                    "ModelID": model_id,
                    "PageName": row.get("Page Display Name", ""),
                    "PageId": row.get("Page Name", ""),
                    "VisualId": row.get("Visual Name", ""),
                    "TableName": row.get("Table Name", ""),
                    "ObjectName": row.get("Object Name", ""),
                    "ObjectType": row.get("Object Type", ""),
                    "FilterType": row.get("Type", ""),
                    "HiddenFilter": str(bool(row.get("Hidden", False))),
                    "LockedFilter": str(bool(row.get("Locked", False))),
                    "displayName": row.get("Filter Name", ""),
                    "HowCreated": row.get("How Created", ""),
                    "Used": str(bool(row.get("Used", False))),
                    "ReportDate": report_date,
                    "WorkspaceName": ws_name
                })
        
        # Visual Objects
        df = rpt.list_visual_objects()
        if isinstance(df, pd.DataFrame) and not df.empty:
            for _, row in df.iterrows():
                result['visual_objects'].append({
                    "ReportName": rpt_name,
                    "ReportID": rpt_id,
                    "ModelID": model_id,
                    "PageName": row.get("Page Display Name", ""),
                    "PageId": row.get("Page Name", ""),
                    "VisualId": row.get("Visual Name", ""),
                    "VisualName": row.get("Visual Name", ""),
                    "VisualType": "",
                    "CustomVisualFlag": str(bool(False)),
                    "TableName": row.get("Table Name", ""),
                    "ObjectName": row.get("Object Name", ""),
                    "ObjectType": row.get("Object Type", ""),
                    "Source": "",
                    "displayName": row.get("Object Display Name", ""),
                    "ImplicitMeasure": str(bool(row.get("Implicit Measure", False))),
                    "Sparkline": str(bool(row.get("Sparkline", False))),
                    "VisualCalc": str(bool(row.get("Visual Calc", False))),
                    "Format": row.get("Format", ""),
                    "ReportDate": report_date,
                    "WorkspaceName": ws_name
                })
        
        # Report-Level Measures
        df = rpt.list_report_level_measures()
        if isinstance(df, pd.DataFrame) and not df.empty:
            for _, row in df.iterrows():
                result['report_level_measures'].append({
                    "ReportName": rpt_name,
                    "ReportID": rpt_id,
                    "ModelID": model_id,
                    "TableName": row.get("Table Name", ""),
                    "ObjectName": row.get("Measure Name", ""),
                    "ObjectType": "Measure",
                    "Expression": row.get("Expression", ""),
                    "HiddenFlag": "False",
                    "FormatString": row.get("Format String", ""),
                    "DataType": row.get("Data Type", ""),
                    "DataCategory": row.get("Data Category", ""),
                    "ReportDate": report_date,
                    "WorkspaceName": ws_name
                })
        
        # Visual Interactions
        df = rpt.list_visual_interactions()
        if isinstance(df, pd.DataFrame) and not df.empty:
            for _, row in df.iterrows():
                result['visual_interactions'].append({
                    "ReportName": rpt_name,
                    "ReportID": rpt_id,
                    "ModelID": model_id,
                    "PageName": row.get("Page Display Name", ""),
                    "PageId": row.get("Page Name", ""),
                    "SourceVisualID": row.get("Source Visual Name", ""),
                    "TargetVisualID": row.get("Target Visual Name", ""),
                    "SourceVisualName": row.get("Source Visual Name", ""),
                    "TargetVisualName": row.get("Target Visual Name", ""),
                    "TypeID": "",
                    "Type": row.get("Type", ""),
                    "ReportDate": report_date,
                    "WorkspaceName": ws_name
                })
    
    except Exception as e:
        result['error'] = str(e)
    
    return result

# ==============================================================  
# GET WORKSPACES
# ==============================================================

workspaces_df = fabric.list_workspaces()

if not SCAN_ALL_WORKSPACES:
    workspaces_df = workspaces_df[workspaces_df["Name"].isin(WORKSPACE_NAMES)]
    if workspaces_df.empty:
        raise ValueError(f"No workspaces found matching: {WORKSPACE_NAMES}")
    log(f"Filtering to workspaces: {WORKSPACE_NAMES}")

log(f"Workspace count: {len(workspaces_df)}")
log("")

# ==============================================================  
# REPORT METADATA EXTRACTION (CROSS-WORKSPACE PARALLEL)
# ==============================================================

# Collect all report tasks across all workspaces first
all_report_tasks = []  # (ws_name, rpt_name, rpt_id, model_id)

if not EXTRACT_REPORT_METADATA:
    log("\nSkipping report metadata extraction (EXTRACT_REPORT_METADATA=False)")
    log("Empty tables will be created with correct schema.")
else:
    for ws_row in workspaces_df.itertuples(index=False):
        ws_name = ws_row.Name
        log(f"\nScanning workspace: {ws_name} | Elapsed: {elapsed_min():.2f} min")

        try:
            reports_df = fabric.list_reports(workspace=ws_name)
            if reports_df is None or reports_df.empty:
                log("  No reports found.")
                continue

            log(f"  Reports found: {len(reports_df)}")
            
            for rpt_row in reports_df.itertuples(index=False):
                rpt_name = rpt_row.Name
                rpt_id = rpt_row.Id
                
                model_id = ""
                if hasattr(rpt_row, 'DatasetId') and rpt_row.DatasetId is not None:
                    model_id = str(rpt_row.DatasetId)
                
                if not model_id:
                    try:
                        dataset_id, _, _, _ = resolve_dataset_from_report(
                            report=rpt_id, workspace=ws_name
                        )
                        model_id = str(dataset_id) if dataset_id is not None else ""
                    except Exception:
                        model_id = ""
                
                all_report_tasks.append((ws_name, rpt_name, rpt_id, model_id))

        except Exception as e:
            log(f"ERROR accessing workspace {ws_name}: {e}")

# Process ALL reports across ALL workspaces in a single parallel pool
if not EXTRACT_REPORT_METADATA:
    pass  # Already logged above
elif all_report_tasks:
    log(f"\nExtracting {len(all_report_tasks)} reports in parallel (max {MAX_PARALLEL_WORKERS} workers)...")
    
    report_results = []
    
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
        futures = {
            executor.submit(extract_report_metadata, ws_name, rpt_name, rpt_id, model_id, REPORT_DATE): (ws_name, rpt_name)
            for ws_name, rpt_name, rpt_id, model_id in all_report_tasks
        }
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            ws_name, rpt_name = futures[future]
            try:
                result = future.result()
                
                if result['error']:
                    log(f"  [{completed}/{len(all_report_tasks)}] ERROR {ws_name}/{rpt_name}: {result['error']}")
                else:
                    report_results.append(result)
                    log(f"  [{completed}/{len(all_report_tasks)}] ✓ {rpt_name}")
            except Exception as e:
                log(f"  [{completed}/{len(all_report_tasks)}] ERROR {rpt_name}: {e}")
    
    # Aggregate all results
    for result in report_results:
        all_connections.extend(result['connections'])
        all_pages.extend(result['pages'])
        all_visuals.extend(result['visuals'])
        all_bookmarks.extend(result['bookmarks'])
        all_custom_visuals.extend(result['custom_visuals'])
        all_report_filters.extend(result['report_filters'])
        all_page_filters.extend(result['page_filters'])
        all_visual_filters.extend(result['visual_filters'])
        all_visual_objects.extend(result['visual_objects'])
        all_report_level_measures.extend(result['report_level_measures'])
        all_visual_interactions.extend(result['visual_interactions'])
else:
    log("No reports to extract.")

# ==============================================================  
# WRITE TO LAKEHOUSE
# ==============================================================

log("\n" + "="*80)
log("Writing output to Lakehouse")
log("="*80)

def write_table(data, name):
    """
    Write data to a Delta table. Schema is inferred from the first row (template).
    Creates empty table with schema if only template row exists.
    
    Args:
        data: List of dictionaries containing the data (first row is schema template)
        name: Name of the table
    """
    full_name = f"{CATALOG}.{LAKEHOUSE_SCHEMA}.{name}"
    
    # Check if we only have the template row (length 1 means just the schema template)
    if len(data) == 1:
        log(f"⚠ No data for {name}, creating empty table with schema")
        # Use template to create empty DataFrame with correct schema
        df = spark.createDataFrame(pd.DataFrame(data))
        # Filter out the template row to create truly empty table
        empty_df = df.filter("1=0")
        empty_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(full_name)
        log(f"✓ Created empty table: {full_name}\n")
        return

    # Skip the template row (first row) and create DataFrame with actual data
    pandas_df = pd.DataFrame(data)
    actual_df = spark.createDataFrame(pandas_df.iloc[1:])

    log(f"Writing {len(data) - 1} rows → {full_name}")

    actual_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(full_name)

    log(f"✓ Wrote table: {full_name}\n")

write_table(all_connections, "Connections")
write_table(all_pages, "Pages")
write_table(all_visuals, "Visuals")
write_table(all_bookmarks, "Bookmarks")
write_table(all_custom_visuals, "CustomVisuals")
write_table(all_report_filters, "ReportFilters")
write_table(all_page_filters, "PageFilters")
write_table(all_visual_filters, "VisualFilters")
write_table(all_visual_objects, "VisualObjects")
write_table(all_report_level_measures, "ReportLevelMeasures")
write_table(all_visual_interactions, "VisualInteractions")

# ==============================================================  
# END
# ==============================================================

heartbeat_running = False

log("\n" + "="*80)
log("PROCESS COMPLETE")
log(f"Finished at: {datetime.now()}")
log(f"Total runtime: {elapsed_min():.2f} minutes")
log("="*80)


# In[4]:


# ================================
# FABRIC DATAFLOW DETAIL EXTRACTOR
# WITH AUTO-SCHEMA CREATION
# ================================
#
# This notebook extracts dataflow detail metadata (queries/entities)
# using Fabric REST APIs, similar to the PowerShell script from:
# https://github.com/chris1642/Power-BI-Backup-Impact-Analysis-Governance-Solution
#
# EXTRACTED DATA (written to lakehouse tables):
# 1. DataflowDetail - dataflow queries with M expressions
#
# Column names match the PowerShell script output:
# - Dataflow ID
# - Dataflow Name
# - Query Name
# - Query (M expression)
# - Report Date
# - Workspace Name - Dataflow Name
# ================================

# %pip install semantic-link-labs --quiet

import time, re, pandas as pd, json, base64
from datetime import datetime
import sempy.fabric as fabric
from sempy.fabric import FabricRestClient

# Uses shared configuration from Cell 0: LAKEHOUSE_SCHEMA, WORKSPACE_NAMES, SCAN_ALL_WORKSPACES

EXTRACTION_TIMESTAMP = datetime.now()
REPORT_DATE = EXTRACTION_TIMESTAMP.strftime("%Y-%m-%d")
start_time = time.time()

# -----------------------------------
# Logging helpers
# -----------------------------------
def log(msg):
    print(msg, flush=True)

def elapsed_min():
    return (time.time() - start_time) / 60

# Heartbeat
import threading
heartbeat_running = True
def heartbeat():
    while heartbeat_running:
        time.sleep(1000)
        print(f"[Heartbeat] Still running… elapsed {elapsed_min():.2f} min", flush=True)

threading.Thread(target=heartbeat, daemon=True).start()

# -----------------------------------
# Start banner
# -----------------------------------
log("="*80)
log("FABRIC DATAFLOW DETAIL EXTRACTION")
log(f"Started: {EXTRACTION_TIMESTAMP}")
log("="*80)

# ============================================
# AUTO-CREATE SCHEMA (LAKEHOUSE)
# ============================================
CATALOG = spark.sql("SELECT current_catalog()").first()[0]
log(f"Using catalog: {CATALOG}")

schema_name = f"{CATALOG}.{LAKEHOUSE_SCHEMA}"
log(f"Ensuring lakehouse schema exists: {schema_name}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
log(f"✓ Schema is ready: {schema_name}\n")

# ==============================================================  


# ==============================================================  
# COLLECTIONS & SCHEMA TEMPLATES
# ==============================================================
# Each collection includes a template row that defines the schema.
# This ensures empty tables can be created with correct column structure.
# Schema matches the PowerShell script output from Final PS Script.txt

all_dataflow_details = [{
    "DataflowId": "",
    "DataflowName": "",
    "QueryName": "",
    "Query": "",
    "ReportDate": "",
    "WorkspaceName": "",
    "WorkspaceNameDataflowName": ""
}]

# ==============================================================  
# HELPER FUNCTIONS
# ==============================================================

def clean_name(name):
    """Clean up names for file/display purposes (matches PowerShell script pattern)"""
    clean = name.replace('[', '(').replace(']', ')')
    clean = re.sub(r'[^a-zA-Z0-9\(\)&,.\- ]', ' ', clean)
    return clean.strip()

def parse_power_query_document(document_content, dataflow_id, dataflow_name, workspace_name, report_date):
    """
    Parse Power Query document content to extract queries.
    Handles both Gen1 and Gen2 dataflow document formats.
    
    Args:
        document_content: The Power Query M document content
        dataflow_id: Dataflow ID
        dataflow_name: Dataflow name
        workspace_name: Workspace name
        report_date: Report date
    
    Returns:
        List of query dictionaries
    """
    queries = []
    
    clean_workspace_name = clean_name(workspace_name)
    clean_dataflow_name = clean_name(dataflow_name)
    workspace_dataflow_name = f"{clean_workspace_name} ~ {clean_dataflow_name}"
    
    # Unescape content if needed (Gen1 dataflows have escaped content)
    document_content = document_content.replace('\\r\\n', '\n').replace('\\n', '\n')
    document_content = document_content.replace('\\"', '"')
    
    # Split by "section Section1;" to get the queries section
    sections = document_content.split('section Section1;', 1)
    
    if len(sections) < 2:
        return queries
    
    queries_section = sections[1]
    
    # Use regex to find all queries in Power Query M document format
    # Pattern breakdown:
    #   (?s)                           - DOTALL mode: dot matches newlines
    #   (?:\[[^\]]*\]\s*)?             - Optional metadata annotations like [IsEnabled=false]
    #   shared\s+                       - "shared" keyword followed by whitespace
    #   (?:#"(.*?)"|([A-Za-z_]\w*))    - Query name: either #"quoted name" (group 1) or unquoted identifier (group 2)
    #   \s*=\s*                         - Assignment operator with optional whitespace
    #   (.*?)                           - Query expression (group 3) - non-greedy capture
    #   (?=...)                         - Lookahead: stop before next "shared" keyword or end of string
    # Supports both: shared QueryName = ... and shared #"Query Name With Spaces" = ...
    pattern = r'(?s)(?:\[[^\]]*\]\s*)?shared\s+(?:#"(.*?)"|([A-Za-z_]\w*))\s*=\s*(.*?)(?=(?:\[[^\]]*\]\s*)?shared\s+(?:#"(?:.*?)"|[A-Za-z_]\w*)\s*=|$)'
    matches = re.findall(pattern, queries_section)
    
    for match in matches:
        # Group 0 = hash-quoted name, Group 1 = unquoted name, Group 2 = expression
        query_name = match[0] if match[0] else match[1]
        query_expression = match[2].strip()
        
        # Remove trailing semicolons
        query_expression = re.sub(r';\s*$', '', query_expression).strip()
        
        # Skip if empty
        if not query_name or not query_expression:
            continue
        
        queries.append({
            "DataflowId": dataflow_id,
            "DataflowName": dataflow_name,
            "QueryName": query_name,
            "Query": query_expression,
            "ReportDate": report_date,
            "WorkspaceName": workspace_name,
            "WorkspaceNameDataflowName": workspace_dataflow_name
        })
    
    return queries

def extract_gen2_dataflow(client, workspace_id, dataflow_id, dataflow_name, workspace_name, report_date):
    """
    Extract Gen2 (Fabric) dataflow definition using getDefinition API.
    
    Args:
        client: FabricRestClient instance
        workspace_id: Workspace ID
        dataflow_id: Dataflow ID
        dataflow_name: Dataflow name
        workspace_name: Workspace name
        report_date: Report date
    
    Returns:
        List of query dictionaries
    """
    queries = []
    
    try:
        # Use Fabric API to get dataflow definition
        endpoint = f"v1/workspaces/{workspace_id}/dataflows/{dataflow_id}/getDefinition"
        response = client.post(endpoint, json={})
        
        if response.status_code != 200:
            return queries
        
        response_data = response.json()
        
        if not response_data.get('definition', {}).get('parts'):
            return queries
        
        # Find the .pq file in the parts
        for part in response_data['definition']['parts']:
            file_path = part.get('path', '')
            payload_type = part.get('payloadType', '')
            payload = part.get('payload', '')
            
            if file_path.endswith('.pq') and payload_type == 'InlineBase64':
                # Decode Base64 content
                try:
                    decoded_bytes = base64.b64decode(payload)
                    pq_content = decoded_bytes.decode('utf-8')
                    
                    # Parse the Power Query document
                    queries = parse_power_query_document(
                        pq_content,
                        dataflow_id,
                        dataflow_name,
                        workspace_name,
                        report_date
                    )
                    break
                except Exception as e:
                    log(f"      Error decoding Gen2 dataflow content: {e}")
    
    except Exception as e:
        log(f"    Could not extract Gen2 dataflow {dataflow_name}: {e}")
    
    return queries

def extract_gen1_dataflow(client, workspace_id, dataflow_id, dataflow_name, workspace_name, report_date):
    """
    Extract Gen1 (Power BI) dataflow definition using REST API.
    
    Args:
        client: FabricRestClient instance
        workspace_id: Workspace ID
        dataflow_id: Dataflow ID
        dataflow_name: Dataflow name
        workspace_name: Workspace name
        report_date: Report date
    
    Returns:
        List of query dictionaries
    """
    queries = []
    
    try:
        # Use Power BI API to get dataflow definition
        api_url = f"v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}"
        response = client.get(api_url)
        
        if response.status_code != 200:
            return queries
        
        dataflow_json = response.json()
        
        # Check for pbi:mashup document content
        if 'pbi:mashup' not in dataflow_json or 'document' not in dataflow_json['pbi:mashup']:
            return queries
        
        document_content = dataflow_json['pbi:mashup']['document']
        
        # Parse the Power Query document
        queries = parse_power_query_document(
            document_content,
            dataflow_id,
            dataflow_name,
            workspace_name,
            report_date
        )
    
    except Exception as e:
        log(f"    Could not extract Gen1 dataflow {dataflow_name}: {e}")
    
    return queries

# ==============================================================  
# GET WORKSPACES
# ==============================================================

workspaces_df = fabric.list_workspaces()

if not SCAN_ALL_WORKSPACES:
    workspaces_df = workspaces_df[workspaces_df["Name"].isin(WORKSPACE_NAMES)]
    if workspaces_df.empty:
        raise ValueError(f"No workspaces found matching: {WORKSPACE_NAMES}")
    log(f"Filtering to workspaces: {WORKSPACE_NAMES}")

log(f"Workspace count: {len(workspaces_df)}")
log("")

# Create REST client instance
client = FabricRestClient()

# ==============================================================  
# DATAFLOW DETAIL EXTRACTION (PARALLELIZED)
# ==============================================================

# Collect all dataflow tasks across workspaces first, then process in parallel
dataflow_extract_tasks = []  # (gen, client, ws_id, df_id, df_name, ws_name, report_date)

for ws_row in workspaces_df.itertuples(index=False):
    ws_name = ws_row.Name
    ws_id = ws_row.Id
    log(f"\nScanning workspace: {ws_name} | Elapsed: {elapsed_min():.2f} min")

    # -------------------- Gen1 Dataflows (Power BI API) --------------------
    try:
        dataflows_url = f"v1.0/myorg/groups/{ws_id}/dataflows"
        response = client.get(dataflows_url)
        
        if response.status_code == 200:
            dataflows = response.json().get('value', [])
            log(f"  Gen1 Dataflows found: {len(dataflows)}")
            for dataflow in dataflows:
                dataflow_extract_tasks.append(("gen1", ws_id, dataflow.get('objectId', ''), dataflow.get('name', ''), ws_name))
        else:
            log(f"  No Gen1 dataflows found")
    except Exception as e:
        log(f"  ERROR fetching Gen1 dataflows: {e}")

    # -------------------- Gen2 Dataflows (Fabric API) --------------------
    try:
        items_url = f"v1/workspaces/{ws_id}/items"
        response = client.get(items_url)
        
        if response.status_code == 200:
            items = response.json().get('value', [])
            gen2_dataflows = [item for item in items if item.get('type') == 'Dataflow']
            log(f"  Gen2 Dataflows found: {len(gen2_dataflows)}")
            for dataflow in gen2_dataflows:
                dataflow_extract_tasks.append(("gen2", ws_id, dataflow.get('id', ''), dataflow.get('displayName', ''), ws_name))
        else:
            log(f"  No Gen2 dataflows found")
    except Exception as e:
        log(f"  ERROR fetching Gen2 dataflows: {e}")

# Process all dataflow extractions in parallel
def extract_dataflow_task(task):
    gen, w_id, df_id, df_name, w_name = task
    if gen == "gen1":
        return extract_gen1_dataflow(client, w_id, df_id, df_name, w_name, REPORT_DATE)
    else:
        return extract_gen2_dataflow(client, w_id, df_id, df_name, w_name, REPORT_DATE)

if dataflow_extract_tasks:
    log(f"\nExtracting {len(dataflow_extract_tasks)} dataflows in parallel (max {MAX_PARALLEL_WORKERS} workers)...")
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
        futures = {
            executor.submit(extract_dataflow_task, task): task
            for task in dataflow_extract_tasks
        }
        for future in as_completed(futures):
            task = futures[future]
            df_name = task[3]
            try:
                queries = future.result()
                if queries:
                    all_dataflow_details.extend(queries)
                    log(f"  ✓ {df_name}: {len(queries)} queries")
                else:
                    log(f"  ✓ {df_name}: no queries")
            except Exception as e:
                log(f"  ERROR extracting {df_name}: {e}")
else:
    log("No dataflows to extract.")

# ==============================================================  
# WRITE TO LAKEHOUSE
# ==============================================================

log("\n" + "="*80)
log("Writing output to Lakehouse")
log("="*80)

def write_table(data, name):
    """
    Write data to a Delta table. Schema is inferred from the first row (template).
    Creates empty table with schema if only template row exists.
    
    Args:
        data: List of dictionaries containing the data (first row is schema template)
        name: Name of the table
    """
    full_name = f"{CATALOG}.{LAKEHOUSE_SCHEMA}.{name}"
    
    # Check if we only have the template row (length 1 means just the schema template)
    if len(data) == 1:
        log(f"⚠ No data for {name}, creating empty table with schema")
        # Use template to create empty DataFrame with correct schema
        df = spark.createDataFrame(pd.DataFrame(data))
        # Filter out the template row to create truly empty table
        empty_df = df.filter("1=0")
        empty_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(full_name)
        log(f"✓ Created empty table: {full_name}\n")
        return

    # Skip the template row (first row) and create DataFrame with actual data
    pandas_df = pd.DataFrame(data)
    actual_df = spark.createDataFrame(pandas_df.iloc[1:])

    log(f"Writing {len(data) - 1} rows → {full_name}")

    actual_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(full_name)

    log(f"✓ Wrote table: {full_name}\n")

write_table(all_dataflow_details, "DataflowDetail")

# ==============================================================  
# END
# ==============================================================

heartbeat_running = False

log("\n" + "="*80)
log("DATA EXTRACTION COMPLETE")
log(f"Finished at: {datetime.now()}")
log(f"Total runtime: {elapsed_min():.2f} minutes")
log("="*80)


# In[5]:


# ================================
# SQL ENDPOINT METADATA REFRESH
# ================================
# After writing tables to the lakehouse, refresh the SQL endpoint metadata
# so that the tables are immediately available for querying via SQL endpoint.
# 
# This uses the Fabric REST API to:
# 1. Get the workspace ID from the notebook context (attached lakehouse)
# 2. List SQL endpoints in the workspace
# 3. Find the SQL endpoint matching the lakehouse name
# 4. Refresh the SQL endpoint metadata
# ================================

log("\n" + "="*80)
log("SQL ENDPOINT METADATA REFRESH")
log(f"Started: {datetime.now()}")
log("="*80)

try:
    log("\nGetting workspace context...")
    
    workspace_id = None
    lakehouse_name = None
    
    # Try to get workspace ID from spark configuration
    try:
        workspace_id = spark.conf.get("trident.workspace.id")
        log(f"  Workspace ID: {workspace_id}")
    except Exception as e:
        log(f"  Could not get workspace ID from spark config: {e}")
    
    # Get the lakehouse name
    try:
        lakehouse_name = spark.conf.get("trident.lakehouse.name")
        log(f"  Lakehouse name: {lakehouse_name}")
    except Exception as e:
        log(f"  Could not get lakehouse name from spark config: {e}")
    
    # If we don't have workspace ID or lakehouse name, we can't proceed
    if not workspace_id:
        log("\n  ERROR: Unable to get workspace ID from notebook context")
        log("  This feature requires running in a Fabric notebook environment.")
        log("\nERROR during SQL endpoint refresh: Unable to get workspace ID")
        log("This is not critical - tables are still written to lakehouse.")
        log("You may need to manually refresh the SQL endpoint if needed.")
    elif not lakehouse_name:
        log("\n  ERROR: Unable to get lakehouse name from notebook context")
        log("\nERROR during SQL endpoint refresh: Unable to get lakehouse name")
        log("This is not critical - tables are still written to lakehouse.")
        log("You may need to manually refresh the SQL endpoint if needed.")
    else:
        # Use FabricRestClient to refresh SQL endpoint
        log(f"\nRefreshing SQL endpoint metadata for lakehouse: {lakehouse_name}")
        
        client = FabricRestClient()
        
        # List SQL endpoints in the workspace
        sql_endpoints_url = f"v1/workspaces/{workspace_id}/sqlEndpoints"
        response = client.get(sql_endpoints_url)
        
        if response.status_code == 200:
            sql_endpoints = response.json().get('value', [])
            log(f"  Found {len(sql_endpoints)} SQL endpoint(s) in workspace")
            
            # Refresh all SQL endpoints in the workspace
            if sql_endpoints:
                log(f"  Refreshing {len(sql_endpoints)} SQL endpoint(s)...")
                
                for endpoint in sql_endpoints:
                    endpoint_name = endpoint.get('displayName', '')
                    endpoint_id = endpoint.get('id', '')
                    
                    # Refresh the SQL endpoint metadata
                    # The API expects a JSON body but all parameters are optional, so we pass an empty object
                    refresh_url = f"v1/workspaces/{workspace_id}/sqlEndpoints/{endpoint_id}/refreshMetadata"
                    refresh_response = client.post(refresh_url, json={})
                    
                    if refresh_response.status_code in [200, 202]:
                        log(f"  ✓ Refreshed SQL endpoint: {endpoint_name}")
                    else:
                        log(f"  Warning: SQL endpoint '{endpoint_name}' refresh returned status {refresh_response.status_code}")
                        log(f"  Response: {refresh_response.text}")
            else:
                log(f"  Warning: No SQL endpoints found in workspace")
        else:
            log(f"  Warning: Could not list SQL endpoints (status {response.status_code})")
            log(f"  Response: {response.text}")
        
        log("\n✓ SQL endpoint metadata refresh completed")

except Exception as e:
    log(f"\nERROR during SQL endpoint refresh: {e}")
    log("This is not critical - tables are still written to lakehouse.")
    log("You may need to manually refresh the SQL endpoint if needed.")

log("\n" + "="*80)
log("ALL PROCESSES COMPLETE")
log(f"Finished at: {datetime.now()}")
log("="*80)
