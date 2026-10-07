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

MAX_PARALLEL_WORKERS = 5

# EXTRACT_MODEL_DEPENDENCIES: Extract measure/column/calc-item dependencies
#     - True = extract dependencies (slower, requires additional XMLA queries per model)
#     - False = skip dependency extraction for faster runs

EXTRACT_MODEL_DEPENDENCIES = True

# EXTRACT_REPORT_METADATA: Extract detailed report metadata via ReportWrapper
#     - True = extract pages, visuals, filters, bookmarks, etc. (slower, opens each report)
#     - False = skip report detail extraction, create empty tables only

EXTRACT_REPORT_METADATA = True

# EXTRACT_FABRIC_APP_QUERIES: Inspect deployed Fabric AppBackend JavaScript for
# static DAX query literals and semantic model mappings. This uses preview
# private endpoints and is currently supported only in the public cloud.
EXTRACT_FABRIC_APP_QUERIES = True
MAX_FABRIC_APP_ASSETS = 64

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

if not isinstance(MAX_FABRIC_APP_ASSETS, int) or MAX_FABRIC_APP_ASSETS < 1 or MAX_FABRIC_APP_ASSETS > 256:
    raise ValueError("MAX_FABRIC_APP_ASSETS must be an integer between 1 and 256.")

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
print(f"  Extract Fabric App Queries: {EXTRACT_FABRIC_APP_QUERIES}")


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
# 15. FabricApps - Fabric AppBackend query-capture status
# 16. FabricAppModels - semantic models configured for Fabric apps
# 17. FabricAppQueries - static DAX candidates found in Fabric app assets
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
import base64
import hashlib
import html
import secrets
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from urllib.parse import quote, urljoin, urlparse
import requests
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
    "Reports": {"WorkspaceId": "", "WorkspaceName": "", "ReportId": "", "ReportName": "", "ReportDescription": "", "ReportWebUrl": "", "ReportEmbedUrl": "", "ReportIsFromPbix": "", "ReportIsOwnedByMe": "", "ReportType": "", "format": "", "DatasetId": "", "DatasetWorkspaceId": "", "DatasetName": "", "users": "", "subscriptions": ""},
    "ReportPages": {"WorkspaceId": "", "WorkspaceName": "", "ReportId": "", "ReportName": "", "PageName": "", "PageDisplayName": "", "PageOrder": 0},
    "Apps": {"AppId": "", "AppName": "", "AppLastUpdate": "", "AppDescription": "", "AppPublishedBy": "", "AppWorkspaceId": "", "WorkspaceName": ""},
    "AppReports": {"AppId": "", "AppName": "", "AppReportId": "", "AppReportType": "", "ReportName": "", "AppReportWebUrl": "", "AppReportEmbedUrl": "", "AppReportIsOwnedByMe": "", "AppReportDatasetId": "", "ReportId": "", "WorkspaceName": ""},
    "FabricApps": {"WorkspaceId": "", "WorkspaceName": "", "AppId": "", "AppName": "", "CaptureStatus": "", "CaptureError": "", "AssetsInspected": 0, "FailedAssets": 0},
    "FabricAppModels": {"AppWorkspaceId": "", "AppWorkspaceName": "", "AppId": "", "AppName": "", "ConnectionAlias": "", "ModelWorkspaceId": "", "ModelWorkspaceName": "", "ModelId": "", "ModelName": "", "Evidence": ""},
    "FabricAppQueries": {"WorkspaceId": "", "WorkspaceName": "", "AppId": "", "AppName": "", "QuerySymbol": "", "Evidence": "", "QueryText": "", "SourceUrl": "", "ConnectionAlias": "", "ModelId": "", "ModelName": "", "ModelWorkspaceId": "", "ModelWorkspaceName": "", "ModelMappingEvidence": ""},
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
    # Dataflow Detail
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
fabric_apps_info = []
fabric_app_models_info = []
fabric_app_queries_info = []

# Lookup tables
dataset_name_lookup = {}
dataflow_name_lookup = {}

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

class _FabricAppHtmlParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.script_sources = []
        self.inline_script_count = 0
        self.has_base = False
        self._inside_script = False
        self._script_has_source = False
        self._inline_parts = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag == "base":
            self.has_base = True
        if tag != "script":
            return
        self._inside_script = True
        self._script_has_source = False
        self._inline_parts = []
        for name, value in attrs:
            if name.lower() == "src" and value:
                self.script_sources.append(html.unescape(value))
                self._script_has_source = True

    def handle_data(self, data):
        if self._inside_script and not self._script_has_source:
            self._inline_parts.append(data)

    def handle_endtag(self, tag):
        if tag.lower() != "script" or not self._inside_script:
            return
        if not self._script_has_source and "".join(self._inline_parts).strip():
            self.inline_script_count += 1
        self._inside_script = False


FABRIC_APP_GUID_PATTERN = (
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)


def _decode_javascript_string(value):
    result = []
    index = 0
    escapes = {
        "n": "\n", "r": "\r", "t": "\t", "b": "\b",
        "f": "\f", "v": "\v", "0": "\0"
    }
    while index < len(value):
        if value[index] != "\\":
            result.append(value[index])
            index += 1
            continue
        index += 1
        if index >= len(value):
            raise ValueError("Incomplete JavaScript escape sequence.")
        code = value[index]
        index += 1
        if code in "\r\n":
            if code == "\r" and index < len(value) and value[index] == "\n":
                index += 1
            continue
        if code in escapes:
            if code == "0" and index < len(value) and value[index].isdigit():
                raise ValueError("Legacy octal escapes are unsupported.")
            result.append(escapes[code])
        elif code == "x":
            hex_value = value[index:index + 2]
            if not re.fullmatch(r"[0-9a-fA-F]{2}", hex_value):
                raise ValueError("Invalid JavaScript hexadecimal escape.")
            result.append(chr(int(hex_value, 16)))
            index += 2
        elif code == "u":
            if index < len(value) and value[index] == "{":
                end = value.find("}", index + 1)
                if end < 0:
                    raise ValueError("Invalid JavaScript Unicode escape.")
                hex_value = value[index + 1:end]
                if not re.fullmatch(r"[0-9a-fA-F]{1,6}", hex_value):
                    raise ValueError("Invalid JavaScript Unicode escape.")
                result.append(chr(int(hex_value, 16)))
                index = end + 1
            else:
                hex_value = value[index:index + 4]
                if not re.fullmatch(r"[0-9a-fA-F]{4}", hex_value):
                    raise ValueError("Invalid JavaScript Unicode escape.")
                result.append(chr(int(hex_value, 16)))
                index += 4
        elif code.isdigit() and code != "0":
            raise ValueError("Legacy JavaScript numeric escapes are unsupported.")
        else:
            result.append(code)
    return "".join(result)


def _extract_fabric_app_catalog(source):
    """Port of Get-FabricAppDaxSource, including its static routing heuristics."""
    # .NET offsets and bounded prefix searches count UTF-16 code units.
    source = "".join(
        character if ord(character) <= 0xFFFF else
        chr(0xD800 + ((ord(character) - 0x10000) >> 10))
        + chr(0xDC00 + ((ord(character) - 0x10000) & 0x3FF))
        for character in source
    )
    queries = []
    script_references = set()
    tokens = []
    lex = re.compile(
        r'''(?P<space>\s+)|(?P<comment>//[^\r\n]*|/\*[\s\S]*?\*/)'''
        r'''|(?P<string>"(?:\\[\s\S]|[^"\\\r\n])*"|'(?:\\[\s\S]|[^'\\\r\n])*')'''
        r'''|(?P<word>[a-zA-Z_$][\w$]*)|(?P<number>\d+(?:\.\d+)?)'''
        r'''|(?P<punct>=>|\+\+|--|&&|\|\||\?\?|[^\s])'''
    )
    regex_literal = re.compile(
        r"/(?:\\[^\r\n]|\[(?:\\[^\r\n]|[^\]\\\r\n])*\]|[^/\\\[\r\n])+/[a-z]*"
    )
    template_chunk = re.compile(r"(?:[^`\\$]+|\\(?:\r\n|[\s\S])|\$(?!\{))+")
    regex_prefix_tokens = {
        "(", "[", "{", ",", ":", ";", "=", "=>", "!", "?", "&&", "||",
        "??", "+", "-", "*", "/", "%", "&", "|", "^", "~", "<", ">",
        "return", "throw", "case", "typeof", "void", "delete", "yield"
    }

    def add_literal(start, end, text, expressions):
        if (
            not expressions
            and re.match(
                r"^(?:\.{0,2}/|https://)\S+\.m?js(?:\?\S*)?$",
                text,
                re.IGNORECASE
            )
        ):
            script_references.add(text)
        if not re.match(r"^\s*(?:EVALUATE|DEFINE)\b", text, re.IGNORECASE):
            return
        prefix = source[max(0, start - 300):start]
        owner_match = re.search(
            r"(?:(?:function\s+(?P<function>[\w$]+)\([^{}]*\)\s*\{\s*return\s*)|"
            r"(?:(?:const|let|var)\s+(?P<variable>[\w$]+)\s*=\s*))$",
            prefix
        )
        symbol = ""
        if owner_match:
            symbol = owner_match.group("function") or owner_match.group("variable") or ""
        if not symbol:
            symbol = f"query_{len(queries) + 1}"
        queries.append({
            "symbol": symbol,
            "evidence": "DynamicTemplate" if expressions else "StaticCandidate",
            "text": text.strip(),
            "expressions": expressions,
            "start": start,
            "end": end,
            "line": 0,
            "column": 0,
            "literalSource": source[start:end],
            "builderSource": None,
            "enclosingExpression": None,
        })

    def scan_code(index, until_brace=False, depth=1):
        if depth > 128:
            raise ValueError("JavaScript nesting limit exceeded.")
        brace_depth = 0
        regex_allowed = True
        while index < len(source):
            start = index
            match = lex.match(source, index)
            if match is None:
                raise ValueError("Could not scan JavaScript token.")
            value = match.group()
            index = match.end()
            if match.lastgroup in ("space", "comment"):
                continue
            if value == "}" and until_brace and brace_depth == 0:
                return index
            if value == "{":
                brace_depth += 1
            if value == "}":
                brace_depth -= 1
            if match.lastgroup == "string":
                text = _decode_javascript_string(value[1:-1])
                tokens.append({"kind": "string", "value": text, "start": start, "end": index})
                add_literal(start, index, text, [])
                regex_allowed = False
                continue
            if value in ('"', "'"):
                raise ValueError("Unterminated JavaScript string.")
            if value == "`":
                parts = []
                expressions = []
                closed = False
                while index < len(source):
                    chunk = template_chunk.match(source, index)
                    if chunk:
                        normalized = chunk.group().replace("\r\n", "\n").replace("\r", "\n")
                        parts.append(_decode_javascript_string(normalized))
                        index = chunk.end()
                    elif source[index] == "`":
                        index += 1
                        closed = True
                        break
                    elif source.startswith("${", index):
                        expression_start = index + 2
                        index = scan_code(expression_start, True, depth + 1)
                        expression = source[expression_start:index - 1]
                        expressions.append(expression)
                        parts.append("${" + expression + "}")
                    else:
                        raise ValueError("Unsupported JavaScript template.")
                if not closed:
                    raise ValueError("Unterminated JavaScript template.")
                text = "".join(parts)
                tokens.append({"kind": "template", "value": text, "start": start, "end": index})
                add_literal(start, index, text, expressions)
                regex_allowed = False
                continue
            if value == "/" and regex_allowed:
                literal = regex_literal.match(source, start)
                if literal is None:
                    raise ValueError("Unterminated or unsupported JavaScript regular expression.")
                index = literal.end()
                tokens.append({"kind": "regex", "value": "", "start": start, "end": index})
                regex_allowed = False
                continue
            tokens.append({
                "kind": "word" if match.lastgroup == "word" else "punct",
                "value": value,
                "start": start,
                "end": index
            })
            regex_allowed = value.lower() in regex_prefix_tokens
        if until_brace:
            raise ValueError("Unterminated JavaScript template expression.")
        return index

    scan_code(0)
    # Retain lexer emission order, including interpolation tokens before their template.
    top_level_tokens = tokens

    def read_object(index, depth=0):
        if depth > 8 or index >= len(tokens) or tokens[index]["value"] != "{":
            raise ValueError("Unsupported model configuration.")
        index += 1
        result = {}
        while index < len(tokens) and tokens[index]["value"] != "}":
            key = tokens[index]
            if key["kind"] not in ("word", "string") or key["value"] in result:
                raise ValueError("Nonliteral or duplicate configuration property.")
            index += 1
            if index >= len(tokens) or tokens[index]["value"] != ":":
                raise ValueError("Unsupported configuration property.")
            index += 1
            if index >= len(tokens):
                raise ValueError("Incomplete configuration.")
            if tokens[index]["kind"] == "string":
                result[key["value"]] = tokens[index]["value"]
                index += 1
            elif tokens[index]["value"] == "{":
                result[key["value"]], index = read_object(index, depth + 1)
            else:
                raise ValueError("Nonliteral model configuration.")
            if index < len(tokens) and tokens[index]["value"] == ",":
                index += 1
            elif index >= len(tokens) or tokens[index]["value"] != "}":
                raise ValueError("Unsupported configuration expression.")
        if index >= len(tokens):
            raise ValueError("Unterminated configuration object.")
        return result, index + 1

    mappings = []
    for token_index in range(len(tokens) - 2):
        if (
            tokens[token_index]["value"].lower() != "semanticmodels"
            or tokens[token_index + 1]["value"] != ":"
            or tokens[token_index + 2]["value"] != "{"
        ):
            continue
        try:
            configuration, _ = read_object(token_index + 2)
        except ValueError as error:
            log(f"    Static Fabric app configuration skipped at offset {tokens[token_index]['start']}: {error}")
            continue
        for alias, model in configuration.items():
            if (
                isinstance(model, dict)
                and isinstance(model.get("workspaceId"), str)
                and isinstance(model.get("itemId"), str)
                and re.fullmatch(FABRIC_APP_GUID_PATTERN, model["workspaceId"])
                and re.fullmatch(FABRIC_APP_GUID_PATTERN, model["itemId"])
            ):
                mappings.append({
                    "connectionAlias": alias,
                    "modelWorkspaceId": model["workspaceId"],
                    "modelId": model["itemId"],
                    "evidence": "StaticConfiguration"
                })

    queries.sort(key=lambda query: query["start"])
    line = 1
    line_start = 0
    scan_offset = 0
    for query in queries:
        while scan_offset < query["start"]:
            newline = source.find("\n", scan_offset)
            if newline < 0 or newline >= query["start"]:
                break
            line += 1
            line_start = newline + 1
            scan_offset = line_start
        query["line"] = line
        query["column"] = query["start"] - line_start + 1

    alias_variables = {}
    for token_index in range(len(top_level_tokens) - 2):
        name, operator, value = top_level_tokens[token_index:token_index + 3]
        if (
            name["kind"] == "word"
            and operator["value"] == "="
            and value["kind"] == "string"
            and re.fullmatch(r"[A-Za-z_$][\w$]*", value["value"])
        ):
            alias_variables[name["value"].casefold()] = value["value"]

    def matching_token(open_index, open_value, close_value):
        depth = 0
        for candidate_index in range(open_index, len(top_level_tokens)):
            value = top_level_tokens[candidate_index]["value"]
            if value == open_value:
                depth += 1
            elif value == close_value:
                depth -= 1
                if depth == 0:
                    return candidate_index
        return -1

    function_ranges = []
    for token_index in range(len(top_level_tokens) - 4):
        if (
            top_level_tokens[token_index]["value"].lower() != "function"
            or top_level_tokens[token_index + 1]["kind"] != "word"
        ):
            continue
        open_parenthesis = token_index + 2
        while (
            open_parenthesis < len(top_level_tokens)
            and top_level_tokens[open_parenthesis]["value"] != "("
        ):
            open_parenthesis += 1
        if open_parenthesis >= len(top_level_tokens):
            continue
        close_parenthesis = matching_token(open_parenthesis, "(", ")")
        if close_parenthesis < 0:
            continue
        open_brace = close_parenthesis + 1
        while open_brace < len(top_level_tokens) and top_level_tokens[open_brace]["value"] != "{":
            open_brace += 1
        if open_brace >= len(top_level_tokens):
            continue
        close_brace = matching_token(open_brace, "{", "}")
        if close_brace < 0:
            continue
        parameters = [
            token["value"]
            for token in top_level_tokens[open_parenthesis + 1:close_parenthesis]
            if token["kind"] == "word"
        ]
        function_ranges.append({
            "name": top_level_tokens[token_index + 1]["value"],
            "parameters": parameters,
            "start_index": token_index,
            "open_index": open_brace,
            "close_index": close_brace,
            "start": top_level_tokens[token_index]["start"],
            "end": top_level_tokens[close_brace]["end"]
        })

    for token_index in range(len(top_level_tokens) - 6):
        if (
            top_level_tokens[token_index]["kind"] != "word"
            or top_level_tokens[token_index + 1]["value"] != "="
            or top_level_tokens[token_index + 2]["value"] != "("
        ):
            continue
        open_parenthesis = token_index + 2
        close_parenthesis = matching_token(open_parenthesis, "(", ")")
        if (
            close_parenthesis < 0
            or close_parenthesis + 2 >= len(top_level_tokens)
            or top_level_tokens[close_parenthesis + 1]["value"] != "=>"
        ):
            continue
        body_open = close_parenthesis + 2
        opening_token = top_level_tokens[body_open]["value"]
        closing_token = {"(": ")", "{": "}", "[": "]"}.get(opening_token)
        if not closing_token:
            continue
        body_close = matching_token(body_open, opening_token, closing_token)
        if body_close < 0:
            continue
        parameters = [
            token["value"]
            for token in top_level_tokens[open_parenthesis + 1:close_parenthesis]
            if token["kind"] == "word"
        ]
        function_ranges.append({
            "name": top_level_tokens[token_index]["value"],
            "parameters": parameters,
            "start_index": token_index,
            "open_index": body_open,
            "close_index": body_close,
            "start": top_level_tokens[token_index]["start"],
            "end": top_level_tokens[body_close]["end"]
        })

    def call_arguments(open_parenthesis):
        arguments = []
        argument_start = open_parenthesis + 1
        round_depth = square_depth = curly_depth = 0
        for token_index in range(argument_start, len(top_level_tokens)):
            value = top_level_tokens[token_index]["value"]
            if value == "(":
                round_depth += 1
            elif value == ")":
                if round_depth == 0 and square_depth == 0 and curly_depth == 0:
                    arguments.append((argument_start, token_index))
                    return arguments
                round_depth -= 1
            elif value == "[":
                square_depth += 1
            elif value == "]":
                square_depth -= 1
            elif value == "{":
                curly_depth += 1
            elif value == "}":
                curly_depth -= 1
            elif (
                value == ","
                and round_depth == 0
                and square_depth == 0
                and curly_depth == 0
            ):
                arguments.append((argument_start, token_index))
                argument_start = token_index + 1
        return arguments

    def resolve_alias(token):
        if (
            token["kind"] == "string"
            and re.fullmatch(r"[A-Za-z_$][\w$]*", token["value"])
        ):
            return token["value"]
        if token["kind"] == "word":
            return alias_variables.get(token["value"].casefold(), "")
        return ""

    def function_at(position):
        matches = [
            function for function in function_ranges
            if function["start"] <= position <= function["end"]
        ]
        if not matches:
            return None
        return min(
            matches,
            key=lambda function: function["end"] - function["start"]
        )

    dependencies_by_function = {}

    def function_dependencies(function):
        if function is None:
            return {}
        key = function["start"]
        if key in dependencies_by_function:
            return dependencies_by_function[key]
        dependencies = {}
        index = function["open_index"] + 1
        while index < function["close_index"] - 2:
            if top_level_tokens[index]["value"].lower() not in ("const", "let", "var"):
                index += 1
                continue
            cursor = index + 1
            while cursor < function["close_index"] - 1:
                if (
                    top_level_tokens[cursor]["kind"] != "word"
                    or top_level_tokens[cursor + 1]["value"] != "="
                ):
                    break
                name = top_level_tokens[cursor]["value"]
                expression_start = cursor + 2
                expression_end = expression_start
                round_depth = square_depth = curly_depth = 0
                while expression_end < function["close_index"]:
                    value = top_level_tokens[expression_end]["value"]
                    if value == "(":
                        round_depth += 1
                    elif value == ")":
                        round_depth -= 1
                    elif value == "[":
                        square_depth += 1
                    elif value == "]":
                        square_depth -= 1
                    elif value == "{":
                        curly_depth += 1
                    elif value == "}":
                        curly_depth -= 1
                    if (
                        round_depth == 0
                        and square_depth == 0
                        and curly_depth == 0
                        and value in (",", ";")
                    ):
                        break
                    expression_end += 1
                dependencies[name.casefold()] = {
                    token["value"]
                    for token in top_level_tokens[expression_start:expression_end]
                    if token["kind"] == "word"
                }
                if (
                    expression_end >= function["close_index"]
                    or top_level_tokens[expression_end]["value"] == ";"
                ):
                    index = expression_end
                    break
                cursor = expression_end + 1
            index += 1
        dependencies_by_function[key] = dependencies
        return dependencies

    def expression_end(start, limit):
        end = start
        round_depth = square_depth = curly_depth = 0
        while end < limit:
            value = top_level_tokens[end]["value"]
            if value == "(":
                round_depth += 1
            elif value == ")":
                if round_depth == 0 and square_depth == 0 and curly_depth == 0:
                    break
                round_depth -= 1
            elif value == "[":
                square_depth += 1
            elif value == "]":
                square_depth -= 1
            elif value == "{":
                curly_depth += 1
            elif value == "}":
                if round_depth == 0 and square_depth == 0 and curly_depth == 0:
                    break
                curly_depth -= 1
            if (
                round_depth == 0
                and square_depth == 0
                and curly_depth == 0
                and value == ","
            ):
                break
            end += 1
        return end

    query_owners = {}
    owner_by_start = {}
    query_aliases = {}
    for query in queries:
        owner_function = function_at(query["start"])
        owner = owner_function["name"] if owner_function else query["symbol"]
        if owner_function is None:
            for index in range(2, len(tokens)):
                if tokens[index]["start"] != query["start"] or tokens[index]["kind"] not in ("string", "template"):
                    continue
                if tokens[index - 1]["value"] == "=" and tokens[index - 2]["kind"] == "word":
                    owner = tokens[index - 2]["value"]
                break
            if owner.lower().startswith("query_"):
                prefix = source[max(0, query["start"] - 500):query["start"]]
                declaration = re.search(r"(?:(?:const|let|var)\s+|,)\s*(?P<name>[a-zA-Z_$][\w$]*)\s*=\s*$", prefix)
                if declaration:
                    owner = declaration["name"]
            if owner.lower().startswith("query_"):
                prefix = source[max(0, query["start"] - 20000):query["start"]]
                declarations = list(re.finditer(r"function\s+(?P<name>[a-zA-Z_$][\w$]*)\s*\(", prefix))
                if declarations:
                    owner = declarations[-1]["name"]
        query_owners.setdefault(owner.casefold(), []).append(query)
        owner_by_start[query["start"]] = owner
        query_aliases[query["start"]] = set()

    def add_expression_route(alias, start, end, function):
        if not alias or start >= end:
            return
        pending = [
            token["value"]
            for token in top_level_tokens[start:end]
            if token["kind"] == "word"
        ]
        for token in top_level_tokens[start:end]:
            for query in queries:
                if token["start"] <= query["start"] and query["end"] <= token["end"]:
                    query_aliases[query["start"]].add(alias)
        dependencies = function_dependencies(function)
        seen = set()
        while pending:
            symbol = pending.pop()
            if symbol in seen:
                continue
            seen.add(symbol)
            for query in query_owners.get(symbol.casefold(), ()):
                query_aliases[query["start"]].add(alias)
            pending.extend(dependencies.get(symbol.casefold(), ()))

    def parameter_index(function, name):
        parameters = function["parameters"]
        return parameters.index(name) if name in parameters else -1

    wrapper_parameter_routes = {}
    parameterized_model_routes = {}
    for function in function_ranges:
        limit = function["close_index"]
        for index in range(function["open_index"] + 1, limit - 5):
            alias = ""
            alias_parameter_index = -1
            query_start = -1
            route_property = tokens[index]["value"].lower()
            if route_property in ("connection", "model") and tokens[index + 1]["value"] == ":":
                alias_token = tokens[index + 2]
                alias = resolve_alias(alias_token)
                if not alias and route_property == "model" and alias_token["kind"] == "word":
                    alias_parameter_index = parameter_index(function, alias_token["value"])
                query_property = "dax" if route_property == "model" else "query"
                for property_index in range(index + 3, min(limit, index + 40)):
                    if tokens[property_index]["value"].lower() == query_property and tokens[property_index + 1]["value"] == ":":
                        query_start = property_index + 2
                        break
            elif route_property == "semanticmodel" and tokens[index + 1]["value"] == "(":
                alias = resolve_alias(tokens[index + 2])
                if (
                    alias and tokens[index + 3]["value"] == ")"
                    and tokens[index + 4]["value"] == "."
                    and tokens[index + 5]["value"].lower() == "query"
                    and tokens[index + 6]["value"] == "("
                ):
                    query_start = index + 7
            if (not alias and alias_parameter_index < 0) or query_start < 0:
                continue
            query_end = expression_end(query_start, limit)
            query_parameter_index = -1
            if query_end == query_start + 1 and tokens[query_start]["kind"] == "word":
                query_parameter_index = parameter_index(function, tokens[query_start]["value"])
            if alias:
                add_expression_route(alias, query_start, query_end, function)
                if query_parameter_index >= 0:
                    wrapper_parameter_routes.setdefault(function["name"].casefold(), {})[query_parameter_index] = alias
            else:
                parameterized_model_routes.setdefault(function["name"].casefold(), []).append({
                    "alias_parameter": alias_parameter_index,
                    "query_start": query_start,
                    "query_end": query_end,
                    "query_parameter": query_parameter_index,
                    "function": function,
                })

    for index in range(len(tokens) - 2):
        name = tokens[index]["value"].casefold()
        if (
            tokens[index + 1]["value"] != "("
            or (index > 0 and tokens[index - 1]["value"].lower() == "function")
            or (name not in parameterized_model_routes and name not in wrapper_parameter_routes)
        ):
            continue
        arguments = call_arguments(index + 1)
        caller = function_at(tokens[index]["start"])
        for route in parameterized_model_routes.get(name, ()):
            if route["alias_parameter"] >= len(arguments):
                continue
            start, end = arguments[route["alias_parameter"]]
            if end != start + 1:
                continue
            alias = resolve_alias(tokens[start])
            if not alias:
                continue
            if 0 <= route["query_parameter"] < len(arguments):
                start, end = arguments[route["query_parameter"]]
                add_expression_route(alias, start, end, caller)
            else:
                add_expression_route(alias, route["query_start"], route["query_end"], route["function"])
        for argument_index, alias in wrapper_parameter_routes.get(name, {}).items():
            if argument_index < len(arguments):
                start, end = arguments[argument_index]
                add_expression_route(alias, start, end, caller)

    def extend_connection_routes():
        # Rayfin hooks may forward (query, connection = default) through several
        # wrappers. Keep the two arguments paired at each call site.
        functions = {function["start"]: function for function in function_ranges}
        parents = {}
        parameters = {}
        summaries = {}
        declarations = {}
        builders = {}

        def scope_at(position):
            function = function_at(position)
            return function["start"] if function else None

        for key, function in functions.items():
            containers = [
                candidate for candidate in function_ranges
                if candidate["start"] < key and function["end"] <= candidate["end"]
            ]
            parent = min(containers, key=lambda candidate: candidate["end"] - candidate["start"]) if containers else None
            parents[key] = parent["start"] if parent else None
            builders.setdefault((parents[key], function["name"]), []).append(key)
            opening = function["start_index"] + 2
            if tokens[opening]["value"] != "(":
                continue
            parsed_parameters = []
            for start, end in call_arguments(opening):
                if start == end:
                    continue
                if tokens[start]["kind"] != "word" or (end > start + 1 and tokens[start + 1]["value"] != "="):
                    break
                parsed_parameters.append((
                    tokens[start]["value"],
                    (start + 2, end) if end > start + 1 else None,
                ))
            else:
                parameters[key] = parsed_parameters
                summaries[key] = set()

        def value_end(start):
            stack = []
            for index in range(start, len(tokens)):
                token = tokens[index]
                if token["kind"] != "punct":
                    continue
                value = token["value"]
                if not stack and value in (",", ";", ")", "]", "}"):
                    return index
                if value in ("(", "[", "{"):
                    stack.append(value)
                elif value in (")", "]", "}"):
                    if stack:
                        stack.pop()
            return len(tokens)

        for index in range(1, len(tokens) - 2):
            if (
                tokens[index]["kind"] == "word"
                and tokens[index + 1]["value"] == "="
                and tokens[index - 1]["value"] in ("const", "let", "var", ",")
            ):
                scope = scope_at(tokens[index]["start"])
                if scope is not None and index < functions[scope]["open_index"]:
                    continue
                declarations[(scope, tokens[index]["value"])] = (index + 2, value_end(index + 2))

        def lookup(name, scope, table):
            while True:
                if (scope, name) in table:
                    return scope, table[(scope, name)]
                if scope is None:
                    return None, None
                scope = parents[scope]

        queries_by_builder = {}
        for query in queries:
            owner = scope_at(query["start"])
            if owner is not None:
                queries_by_builder.setdefault(owner, set()).add(("query", query["start"]))

        def evaluate(start, end, scope, kind, seen=frozenset()):
            if start >= end:
                return set()
            expression = tokens[start:end]
            if kind == "alias":
                if len(expression) != 1:
                    return set()
                token = expression[0]
                if token["kind"] == "string":
                    return {("alias", token["value"])} if re.fullmatch(r"[A-Za-z_$][\w$]*", token["value"]) else set()
            result = set()
            if kind == "query":
                for token in expression:
                    if token["kind"] in ("string", "template"):
                        result.update(
                            ("query", query["start"]) for query in queries
                            if token["start"] <= query["start"] and query["end"] <= token["end"]
                        )
            for index in range(start, end):
                token = tokens[index]
                if token["kind"] != "word" or (index > start and tokens[index - 1]["value"] in (".", "?.")):
                    continue
                name = token["value"]
                param_names = [param[0] for param in parameters.get(scope, ())]
                if name in param_names:
                    result.add(("parameter", param_names.index(name)))
                    continue
                declaration_scope, declaration = lookup(name, scope, declarations)
                if declaration is not None:
                    key = (declaration_scope, name, kind)
                    if key not in seen:
                        result.update(evaluate(*declaration, declaration_scope, kind, seen | {key}))
                    continue
                if kind == "query" and index + 1 < end and tokens[index + 1]["value"] == "(":
                    _, candidates = lookup(name, scope, builders)
                    if candidates and len(candidates) == 1:
                        result.update(queries_by_builder.get(candidates[0], ()))
            return result

        # Only infer this extension from a connection/query object with a
        # parameterized connection, not arbitrary two-argument function calls.
        for index in range(len(tokens) - 4):
            if tokens[index]["value"] != "{" or tokens[index]["kind"] != "punct":
                continue
            scope = scope_at(tokens[index]["start"])
            if scope not in parameters:
                continue
            end = matching_token(index, "{", "}")
            if end < 0:
                continue
            properties = {}
            cursor = index + 1
            while cursor < end:
                if tokens[cursor]["kind"] not in ("word", "string"):
                    break
                name = tokens[cursor]["value"]
                if cursor + 1 < end and tokens[cursor + 1]["value"] == ":":
                    start = cursor + 2
                    stop = min(value_end(start), end)
                else:
                    start, stop = cursor, cursor + 1
                if name in properties:
                    break
                properties[name] = (start, stop)
                cursor = stop
                if cursor < end and tokens[cursor]["value"] == ",":
                    cursor += 1
                elif cursor != end:
                    break
            else:
                if "connection" not in properties or "query" not in properties:
                    continue
                aliases = evaluate(*properties["connection"], scope, "alias")
                query_values = evaluate(*properties["query"], scope, "query")
                for alias in aliases:
                    if alias[0] == "parameter":
                        summaries[scope].update((alias, query_value) for query_value in query_values)

        calls = []
        wrapper_names = {functions[key]["name"] for key in parameters}
        for index in range(len(tokens) - 2):
            token = tokens[index]
            if (
                token["kind"] != "word" or token["value"] not in wrapper_names
                or tokens[index + 1]["value"] != "("
                or (index > 0 and tokens[index - 1]["value"] in ("function", ".", "?."))
            ):
                continue
            scope = scope_at(token["start"])
            _, candidates = lookup(token["value"], scope, builders)
            if candidates and len(candidates) == 1 and candidates[0] in summaries:
                calls.append((scope, candidates[0], call_arguments(index + 1)))

        def bind(value, arguments, caller, callee, kind):
            if value[0] != "parameter":
                return {value}
            index = value[1]
            if index < len(arguments):
                start, end = arguments[index]
                if start < end and not (end == start + 1 and tokens[start]["value"] == "undefined"):
                    return evaluate(start, end, caller, kind)
            default = parameters[callee][index][1]
            return evaluate(*default, parents[callee], kind) if default else set()

        changed = True
        while changed:
            changed = False
            for caller, callee, arguments in calls:
                for alias, query_value in tuple(summaries[callee]):
                    for actual_alias in bind(alias, arguments, caller, callee, "alias"):
                        for actual_query in bind(query_value, arguments, caller, callee, "query"):
                            if actual_alias[0] == "alias" and actual_query[0] == "query":
                                query_aliases[actual_query[1]].add(actual_alias[1])
                            elif caller in summaries:
                                route = (actual_alias, actual_query)
                                if route not in summaries[caller]:
                                    summaries[caller].add(route)
                                    changed = True

    extend_connection_routes()

    for query in queries:
        aliases = query_aliases[query["start"]]
        prefix = source[max(0, query["start"] - 500):query["start"]]
        model_matches = list(re.finditer(
            r'''model\s*:\s*["'](?P<route>[a-zA-Z_$][\w$]*)["']\s*,\s*dax\s*:\s*$''', prefix
        ))
        if model_matches:
            aliases.add(model_matches[-1]["route"])
        wrapper_matches = list(re.finditer(
            r'''(?<![\w$])[a-zA-Z_$][\w$]*\s*\(\s*["'](?P<route>[a-zA-Z_$][\w$]*)["']\s*,''', prefix
        ))
        if wrapper_matches:
            aliases.add(wrapper_matches[-1]["route"])
        owner = owner_by_start[query["start"]]
        if owner and not owner.lower().startswith("query_"):
            owner_pattern = r"(?<![\w$])" + re.escape(owner) + r"(?![\w$])"
            for match in re.finditer(
                owner_pattern + r'''\s*\(\s*["'](?P<route>[a-zA-Z_$][\w$]*)["']\s*,''', source
            ):
                aliases.add(match["route"])
            for call in re.finditer(owner_pattern + r"\s*\(", source):
                prefix = source[max(0, call.start() - 600):call.start()]
                assignments = list(re.finditer(r"(?P<symbol>[a-zA-Z_$][\w$]*)\s*=", prefix))
                if not assignments:
                    continue
                symbol = re.escape(assignments[-1]["symbol"])
                for match in re.finditer(
                    r'''connection\s*:\s*["'](?P<route>[a-zA-Z_$][\w$]*)["']\s*,\s*query\s*:\s*'''
                    + symbol + r"(?![\w$])", source
                ):
                    aliases.add(match["route"])
        query["connectionAliases"] = sorted(aliases, key=str.casefold)

    def unicode_text(value):
        return value.encode("utf-16-le", errors="surrogatepass").decode(
            "utf-16-le", errors="surrogatepass"
        )

    for query in queries:
        for field in ("text", "literalSource"):
            query[field] = unicode_text(query[field])
        query["expressions"] = [unicode_text(value) for value in query["expressions"]]
    return {
        "queries": queries,
        "scriptReferences": sorted(unicode_text(value) for value in script_references),
        "modelMappings": mappings
    }


def _fabric_app_token(resource):
    from notebookutils import mssparkutils
    token = mssparkutils.credentials.getToken(resource)
    if not token:
        raise RuntimeError(f"Could not acquire an access token for {resource}.")
    return token


def _fabric_app_request(session, method, url, headers=None, json_body=None):
    response = session.request(
        method,
        url,
        headers=headers or {},
        json=json_body,
        allow_redirects=False,
        timeout=60
    )
    if response.status_code != 200:
        raise RuntimeError(f"Request failed with HTTP {response.status_code}: {url}")
    if len(response.content) > 20 * 1024 * 1024:
        raise RuntimeError(f"Response exceeded the 20 MB asset limit: {url}")
    return response


def _fabric_rest_json(client, path):
    response = client.get(path)
    if response.status_code != 200:
        raise RuntimeError(f"Fabric request failed with HTTP {response.status_code}: {path}")
    if len(response.content) > 20 * 1024 * 1024:
        raise RuntimeError(f"Fabric response exceeded the 20 MB limit: {path}")
    return response.json()


def _validated_fabric_app_uri(value, host_suffix):
    parsed = urlparse(value)
    if parsed.scheme != "https" or parsed.username or parsed.password or parsed.port not in (None, 443):
        raise ValueError("Fabric App endpoint must be a standard HTTPS URL.")
    host = (parsed.hostname or "").lower()
    if not host.endswith(host_suffix.lower()) or host == host_suffix.lstrip(".").lower():
        raise ValueError(f"Unexpected Fabric App endpoint host: {host}")
    return parsed


def _map_fabric_app_queries(queries, models):
    """Canonicalize uniquely matched aliases before candidate filtering and model mapping."""
    configured_aliases = {model["ConnectionAlias"] for model in models}
    if configured_aliases:
        aliases_by_case = {}
        for alias in configured_aliases:
            aliases_by_case.setdefault(alias.casefold(), []).append(alias)
        groups = {}
        for query in queries:
            alias = query["ConnectionAlias"]
            if alias and alias not in configured_aliases:
                matches = aliases_by_case.get(alias.casefold(), [])
                if len(matches) == 1:
                    query["ConnectionAlias"] = matches[0]
            key = "\0".join(query[field] for field in ("SourceUrl", "QuerySymbol", "QueryText"))
            groups.setdefault(key.casefold(), []).append(query)
        queries = []
        for candidates in groups.values():
            valid = [
                query for query in candidates
                if query["ConnectionAlias"] and query["ConnectionAlias"] in configured_aliases
            ]
            if valid:
                unique_candidates = {}
                for query in valid:
                    unique_candidates.setdefault(query["ConnectionAlias"], query)
                queries.extend(unique_candidates.values())
            else:
                candidates[0]["ConnectionAlias"] = ""
                queries.append(candidates[0])

    unique_models = {}
    for model in models:
        fields = ("ConnectionAlias", "ModelId", "ModelWorkspaceId")
        if all(model[field] for field in fields):
            key = tuple(model[field].casefold() for field in fields)
            unique_models.setdefault(key, model)
    configured_models = list(unique_models.values())
    for query in queries:
        alias = query["ConnectionAlias"]
        matches = (
            [model for model in configured_models if model["ConnectionAlias"] == alias]
            if alias else configured_models
        )
        mapping = matches[0] if len(matches) == 1 else None
        query["ModelMappingEvidence"] = (
            ("ConnectionAlias" if alias else "SingleConfiguredModel") if mapping else ""
        )
        if mapping:
            query["ConnectionAlias"] = mapping["ConnectionAlias"]
        for field in ("ModelId", "ModelName", "ModelWorkspaceId", "ModelWorkspaceName"):
            query[field] = mapping[field] if mapping else ""
    return queries


def _capture_fabric_app_queries(app_item, dataset_lookup, workspace_lookup):
    workspace_id = app_item["WorkspaceId"]
    workspace_name = app_item["WorkspaceName"]
    app_id = app_item["FabricItemID"]
    app_name = app_item["FabricItemName"]
    capture = {
        "WorkspaceId": workspace_id,
        "WorkspaceName": workspace_name,
        "AppId": app_id,
        "AppName": app_name,
        "CaptureStatus": "Failed",
        "CaptureError": "",
        "AssetsInspected": 0,
        "FailedAssets": 0
    }
    queries = []
    mappings = []
    stage = "Discovery"
    session = requests.Session()
    try:
        fabric_client = FabricRestClient()
        item_endpoint = f"v1/workspaces/{workspace_id}/appBackends/{app_id}"
        deployment = _fabric_rest_json(
            fabric_client, f"{item_endpoint}/__private/deploy/status"
        )
        extended = _fabric_rest_json(
            fabric_client, f"{item_endpoint}/__private/extended-properties"
        )
        hosting = _validated_fabric_app_uri(deployment["hostingUrl"], ".fabricapps.net")
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.webapp(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)*\.fabricapps\.net", hosting.hostname or ""):
            raise ValueError("Unexpected Fabric App hosting URL.")
        workload = _validated_fabric_app_uri(
            extended["extendedProperties"]["BaaSEndpoint"],
            ".pbidedicated.windows.net"
        )
        if hosting.path not in ("", "/"):
            raise ValueError(
                f"Fabric App hosting URL has an unexpected path: {hosting.path}"
            )
        expected_workload_path = (
            f"/workspaces/{workspace_id}/appbackends/{app_id}".lower()
        )
        if not workload.path.rstrip("/").lower().endswith(expected_workload_path):
            raise ValueError(
                "Fabric App workload URL does not match the requested workspace and app."
            )
        origin = f"{hosting.scheme}://{hosting.netloc}"

        stage = "Workload authentication"
        power_bi_token = _fabric_app_token("https://analysis.windows.net/powerbi/api")
        power_bi_headers = {"Authorization": f"Bearer {power_bi_token}"}
        cluster_info = _fabric_app_request(
            session, "GET", "https://api.powerbi.com/metadata/cluster", power_bi_headers
        ).json()
        cluster = _validated_fabric_app_uri(cluster_info["backendUrl"], ".analysis.windows.net")
        cluster_origin = f"{cluster.scheme}://{cluster.netloc}"
        power_bi_headers["X-PowerBI-ResourceKey"] = workspace_id
        claims = {
            "workspaceObjectId": workspace_id,
            "workloadType": "BaaS",
            "artifacts": [{"artifactType": "AppBackend", "artifactObjectId": app_id}]
        }
        mwc = _fabric_app_request(
            session,
            "POST",
            f"{cluster_origin}/metadata/v201606/generatemwctokenv2",
            power_bi_headers,
            claims
        ).json()
        if not mwc.get("Token") or mwc.get("TargetUriHost", "").lower() != (workload.hostname or "").lower():
            raise ValueError("Workload token target mismatch.")

        workload_headers = {
            "Authorization": f"MwcToken {mwc['Token']}",
            "x-ms-workload-resource-moniker": app_id
        }
        runtime_warning = ""
        try:
            runtime_url = f"{extended['extendedProperties']['BaaSEndpoint'].rstrip('/')}/api/projectRuntimeSettings"
            runtime_settings = _fabric_app_request(
                session, "GET", runtime_url, workload_headers
            ).json()
            connectors = runtime_settings.get("serviceSettings", {}).get("connectors", {})
            for alias, connector in connectors.items():
                if connector.get("connector") != "fabric-semanticmodel":
                    continue
                config = connector.get("config", {})
                model_id = config.get("itemId", "")
                model_workspace_id = config.get("workspaceId", "")
                if (
                    re.fullmatch(FABRIC_APP_GUID_PATTERN, model_id, re.IGNORECASE)
                    and re.fullmatch(FABRIC_APP_GUID_PATTERN, model_workspace_id, re.IGNORECASE)
                ):
                    mappings.append({
                        "connectionAlias": alias,
                        "modelId": model_id,
                        "modelWorkspaceId": model_workspace_id,
                        "evidence": "BackendConnectorConfiguration"
                    })
        except Exception as error:
            runtime_warning = f"Runtime connector settings were unavailable: {error}"

        stage = "Hosting authentication"
        verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")
        challenge = base64.urlsafe_b64encode(
            hashlib.sha256(verifier.encode("ascii")).digest()
        ).decode().rstrip("=")
        state = uuid.uuid4().hex
        handoff = _fabric_app_request(
            session,
            "POST",
            f"{extended['extendedProperties']['BaaSEndpoint'].rstrip('/')}/api/auth/v1/brokered/authorize",
            workload_headers,
            {
                "returnOrigin": origin,
                "codeChallenge": challenge,
                "codeChallengeMethod": "S256",
                "state": state
            }
        ).json()
        if not handoff.get("handoffCode") or handoff.get("state") != state:
            raise ValueError("Hosting handoff state mismatch.")
        _fabric_app_request(
            session,
            "GET",
            f"{origin}/?_hc={quote(handoff['handoffCode'], safe='')}",
            {
                "Accept": "application/json",
                "x-rayfin-sh-code-verifier": verifier,
                "x-ms-workload-resource-moniker": app_id
            }
        )

        stage = "Asset discovery"
        app_html = _fabric_app_request(
            session, "GET", f"{origin}/", {"Accept": "text/html"}
        ).text
        if re.search(r"<title>\s*Sign in required\s*</title>", app_html, re.IGNORECASE):
            raise RuntimeError("Hosting session did not serve the Fabric app.")
        parser = _FabricAppHtmlParser()
        parser.feed(app_html)
        pending = [urljoin(f"{origin}/", source) for source in parser.script_sources]
        seen = set()
        failed_assets = parser.inline_script_count + int(parser.has_base or not parser.script_sources)
        failure_details = []
        if parser.inline_script_count:
            failure_details.append(
                f"{parser.inline_script_count} inline script(s) require manual review"
            )
        if parser.has_base:
            failure_details.append("HTML base URL requires manual review")
        if not parser.script_sources:
            failure_details.append("no external script sources were discovered")
        while pending and capture["AssetsInspected"] < MAX_FABRIC_APP_ASSETS:
            asset_url = pending.pop(0)
            if asset_url in seen:
                continue
            seen.add(asset_url)
            asset = urlparse(asset_url)
            if (
                f"{asset.scheme}://{asset.netloc}" != origin
                or asset.username
                or asset.password
                or asset.query
                or asset.fragment
            ):
                failed_assets += 1
                failure_details.append(f"rejected asset URL: {asset_url}")
                continue
            capture["AssetsInspected"] += 1
            try:
                response = _fabric_app_request(session, "GET", asset_url)
                if "text/html" in response.headers.get("Content-Type", "").lower():
                    raise RuntimeError("Expected JavaScript but received HTML.")
                catalog = _extract_fabric_app_catalog(response.text)
                if catalog["queries"]:
                    candidate_aliases = sorted({
                        alias
                        for query in catalog["queries"]
                        for alias in query["connectionAliases"]
                        if alias
                    })
                    log(
                        f"    Parsed {len(catalog['queries'])} DAX candidates from "
                        f"{asset.path.rsplit('/', 1)[-1] or '/'}; "
                        f"candidate aliases: {candidate_aliases or ['<none>']}"
                    )
                backend_aliases = {
                    mapping["connectionAlias"]
                    for mapping in mappings
                    if mapping["evidence"] == "BackendConnectorConfiguration"
                }
                for mapping in catalog["modelMappings"]:
                    if mapping["connectionAlias"] not in backend_aliases:
                        mappings.append(mapping)
                for query in catalog["queries"]:
                    aliases = query["connectionAliases"] or [""]
                    for alias in aliases:
                        queries.append({
                            "WorkspaceId": workspace_id,
                            "WorkspaceName": workspace_name,
                            "AppId": app_id,
                            "AppName": app_name,
                            "QuerySymbol": query["symbol"],
                            "Evidence": query["evidence"],
                            "QueryText": query["text"],
                            "SourceUrl": asset_url,
                            "ConnectionAlias": alias,
                            "ModelId": "",
                            "ModelName": "",
                            "ModelWorkspaceId": "",
                            "ModelWorkspaceName": "",
                            "ModelMappingEvidence": ""
                        })
                pending.extend(urljoin(asset_url, reference) for reference in catalog["scriptReferences"])
            except Exception as error:
                failed_assets += 1
                failure_details.append(f"{asset_url}: {error}")
                log(f"    Warning: Fabric app asset skipped ({asset_url}): {error}")

        capture["FailedAssets"] = failed_assets
        warnings = []
        if runtime_warning:
            warnings.append(runtime_warning)
        if failed_assets or pending:
            detail = "; ".join(failure_details[:3])
            if pending:
                detail = f"{detail}; {len(pending)} asset(s) remained after the limit".strip("; ")
            warnings.append(
                "Some scripts were skipped or unavailable, or the asset limit was reached."
                + (f" Details: {detail}" if detail else "")
            )
        if not queries:
            warnings.append(
                "No DAX literals were found; static inspection does not prove that the app has no queries."
            )

        unique_mappings = {}
        for mapping in mappings:
            if not all(mapping[field] for field in ("connectionAlias", "modelWorkspaceId", "modelId")):
                continue
            key = (
                mapping["connectionAlias"],
                mapping["modelWorkspaceId"],
                mapping["modelId"],
                mapping["evidence"],
            )
            unique_mappings.setdefault(key, mapping)
        models = []
        resolved_workspaces = dict(workspace_lookup)
        resolved_datasets = dict(dataset_lookup)
        for mapping in unique_mappings.values():
            model_workspace_id = mapping["modelWorkspaceId"]
            model_id = mapping["modelId"]
            if model_workspace_id not in resolved_workspaces:
                try:
                    workspace_metadata = _fabric_rest_json(
                        fabric_client, f"v1/workspaces/{model_workspace_id}"
                    )
                    if (
                        workspace_metadata.get("id", "").lower() == model_workspace_id.lower()
                        and workspace_metadata.get("displayName")
                    ):
                        resolved_workspaces[model_workspace_id] = workspace_metadata["displayName"]
                except Exception as error:
                    warnings.append(
                        f"Could not resolve model workspace {model_workspace_id}: {error}"
                    )
            dataset_key = (model_workspace_id, model_id)
            if dataset_key not in resolved_datasets:
                try:
                    model_metadata = _fabric_rest_json(
                        fabric_client,
                        f"v1/workspaces/{model_workspace_id}/items/{model_id}"
                    )
                    if (
                        model_metadata.get("id", "").lower() == model_id.lower()
                        and model_metadata.get("type") == "SemanticModel"
                        and model_metadata.get("displayName")
                    ):
                        resolved_datasets[dataset_key] = model_metadata["displayName"]
                except Exception as error:
                    warnings.append(
                        f"Could not resolve semantic model {model_id}: {error}"
                    )
            models.append({
                "AppWorkspaceId": workspace_id,
                "AppWorkspaceName": workspace_name,
                "AppId": app_id,
                "AppName": app_name,
                "ConnectionAlias": mapping["connectionAlias"],
                "ModelWorkspaceId": model_workspace_id,
                "ModelWorkspaceName": resolved_workspaces.get(model_workspace_id, ""),
                "ModelId": model_id,
                "ModelName": resolved_datasets.get(dataset_key, ""),
                "Evidence": mapping["evidence"]
            })

        configured_aliases = {model["ConnectionAlias"] for model in models}
        log(
            f"    Configured Fabric app aliases: "
            f"{sorted(configured_aliases) or ['<none>']}"
        )
        queries = _map_fabric_app_queries(queries, models)
        if not models:
            warnings.append(
                "No semantic-model targets were discovered in backend connector settings or app scripts; query model fields remain blank."
            )

        capture["CaptureStatus"] = "Partial" if warnings else "Succeeded"
        capture["CaptureError"] = " ".join(warnings)
        return capture, models, queries
    except Exception as error:
        capture["CaptureError"] = (
            f"{stage} failed: {error}. Check sign-in, app permissions, "
            "prerequisites, and preview API availability."
        )
        return capture, [], []
    finally:
        session.close()

# ==============================================================  
# PARALLEL API HELPERS FOR PERFORMANCE
# ==============================================================
# These helpers enable parallel fetching of dataset details
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

    # -------------------- REPORTS (REST, with parallel page fetching) --------------------
    # Uses the raw Power BI REST API (like the PowerShell script) so EVERY report
    # field is captured - including isFromPbix, isOwnedByMe, format, datasetWorkspaceId
    # and passthrough fields (users, subscriptions) - rather than the curated subset
    # returned by fabric.list_reports().
    try:
        log(f"  Fetching reports...")
        reports_value = []
        reports_resp = client.get(f"v1.0/myorg/groups/{ws_id}/reports")
        if reports_resp.status_code == 200:
            reports_value = reports_resp.json().get("value", [])

        if reports_value:
            log(f"  Reports found: {len(reports_value)}")

            # Collect report info and page tasks
            page_tasks = []
            for rpt in reports_value:
                report_id = rpt.get("id", "")
                report_name = rpt.get("name", "")
                dataset_id = rpt.get("datasetId", "")

                # Get dataset name from lookup
                dataset_name = dataset_name_lookup.get(dataset_id, "Unknown Dataset")

                reports_info.append({
                    "WorkspaceId": ws_id,
                    "WorkspaceName": ws_name,
                    "ReportId": report_id,
                    "ReportName": report_name,
                    "ReportDescription": rpt.get("description", ""),
                    "ReportWebUrl": rpt.get("webUrl", ""),
                    "ReportEmbedUrl": rpt.get("embedUrl", ""),
                    "ReportIsFromPbix": str(bool(rpt.get("isFromPbix", False))),
                    "ReportIsOwnedByMe": str(bool(rpt.get("isOwnedByMe", False))),
                    "ReportType": rpt.get("reportType", ""),
                    "format": rpt.get("format", ""),
                    "DatasetId": dataset_id,
                    "DatasetWorkspaceId": rpt.get("datasetWorkspaceId", ""),
                    "DatasetName": dataset_name,
                    "users": serialize_json(rpt.get("users")),
                    "subscriptions": serialize_json(rpt.get("subscriptions"))
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
# FABRIC APP QUERIES AND MODEL MAPPINGS
# ==============================================================

log("\n" + "="*80)
log("Fetching Fabric App Queries and Model Mappings")
log("="*80)

fabric_app_items = [
    item for item in fabric_items_info
    if item["FabricItemType"] == "AppBackend"
]

if not EXTRACT_FABRIC_APP_QUERIES:
    log("Fabric app query extraction is disabled")
elif not fabric_app_items:
    log("No Fabric AppBackend items found")
else:
    fabric_app_dataset_lookup = {
        (dataset["WorkspaceId"], dataset["DatasetId"]): dataset["DatasetName"]
        for dataset in datasets_info
    }
    fabric_app_workspace_lookup = {
        workspace["WorkspaceId"]: workspace["WorkspaceName"]
        for workspace in workspaces_info
    }
    log(
        f"Extracting queries from {len(fabric_app_items)} Fabric apps "
        f"(max {MAX_WORKERS} workers)..."
    )
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                _capture_fabric_app_queries,
                app_item,
                fabric_app_dataset_lookup,
                fabric_app_workspace_lookup
            ): app_item
            for app_item in fabric_app_items
        }
        for future in as_completed(futures):
            app_item = futures[future]
            try:
                capture, models, queries = future.result()
            except Exception as e:
                capture = {
                    "WorkspaceId": app_item["WorkspaceId"],
                    "WorkspaceName": app_item["WorkspaceName"],
                    "AppId": app_item["FabricItemID"],
                    "AppName": app_item["FabricItemName"],
                    "CaptureStatus": "Failed",
                    "CaptureError": f"Unexpected extraction failure: {e}",
                    "AssetsInspected": 0,
                    "FailedAssets": 0
                }
                models = []
                queries = []
            fabric_apps_info.append(capture)
            fabric_app_models_info.extend(models)
            fabric_app_queries_info.extend(queries)
            log(
                f"  {capture['CaptureStatus']}: {capture['WorkspaceName']} ~ "
                f"{capture['AppName']} ({len(queries)} queries, {len(models)} models)"
            )
            if capture["CaptureError"]:
                log(f"    {capture['CaptureError']}")

# ==============================================================
# WRITE TO LAKEHOUSE
# ==============================================================

log("\n" + "="*80)
log("Writing output to Lakehouse")
log("="*80)

def write_table(data, name):
    full_name = f"{CATALOG}.{LAKEHOUSE_SCHEMA}.{name}"
    
    if not data:
        log(f"No data for {name}, skipping (already cleared at startup)\n")
        return

    # Convert to pandas DataFrame first for proper type handling, then to Spark
    pandas_df = pd.DataFrame(data)
    df = spark.createDataFrame(pandas_df)

    log(f"Writing {len(data)} rows → {full_name}")

    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(full_name)

    log(f"✓ Wrote table: {full_name}\n")

# Write all tables
write_table(workspaces_info, "Workspaces")
write_table(fabric_items_info, "FabricItems")
write_table(datasets_info, "Datasets")
write_table(dataset_sources_info, "DatasetSourcesInfo")
write_table(dataset_refresh_history, "DatasetRefreshHistory")
write_table(dataset_refresh_schedule, "DatasetRefreshSchedule")
write_table(dataflows_info, "Dataflows")
write_table(dataflow_lineage, "DataflowLineage")
write_table(dataflow_sources_info, "DataflowSourcesInfo")
write_table(dataflow_refresh_history, "DataflowRefreshHistory")
write_table(reports_info, "Reports")
write_table(report_pages_info, "ReportPages")
write_table(apps_info, "Apps")
write_table(reports_in_app_info, "AppReports")
write_table(fabric_apps_info, "FabricApps")
write_table(fabric_app_models_info, "FabricAppModels")
write_table(fabric_app_queries_info, "FabricAppQueries")

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
            cross = r.CrossFilteringBehavior.ToString()
            arrow = "<-->" if cross == "BothDirections" else "->"
            expr = f"'{r.FromTable.Name}'[{r.FromColumn.Name}] {arrow} '{r.ToTable.Name}'[{r.ToColumn.Name}]"
            details.append({
                "Type": "Relationship", "Table": r.FromTable.Name, "Name": r.FromColumn.Name,
                "FormatString": "", "DisplayFolder": "", "Description": "",
                "IsHidden": "", "TableStorageMode": "",
                "Expression": expr,
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

import time, re, math, json, base64, pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import sempy.fabric as fabric
from sempy.fabric import FabricRestClient
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
# NON-PBIR (CLASSIC LAYOUT) PARSER + getDefinition FETCH HELPERS
# ==============================================================
# Reports authored in the classic (non-PBIR) format are not readable by
# ReportWrapper. For those, we fetch the report definition the same way the
# PowerShell solution does (Fabric getDefinition API), take the legacy
# "report.json" (which IS the classic Report/Layout), and parse it in-memory
# with the port of "Report Detail Extract Script.csx" below. Connections are
# synthesized from "definition.pbir" (datasetReference) exactly like the PS.

def dig(obj, *path):
    cur = obj
    for key in path:
        if cur is None:
            return None
        if isinstance(key, int):
            cur = cur[key] if isinstance(cur, list) and -len(cur) <= key < len(cur) else None
        else:
            cur = cur[key] if isinstance(cur, dict) and key in cur else None
    return cur


def digs(obj, *path):
    v = dig(obj, *path)
    if v is None or isinstance(v, (dict, list)):
        return None
    if isinstance(v, bool):
        return "True" if v else "False"
    return str(v)


def parse_json_field(parent, key):
    raw = dig(parent, key)
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    if not isinstance(raw, str) or raw.strip() == "":
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def to_int(v, default=0):
    if v is None:
        return default
    try:
        return int(math.ceil(float(v)))
    except (TypeError, ValueError):
        return default


def as_bool(v, default=False):
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1")
    return default


def find_field(node):
    if isinstance(node, dict):
        for obj_type in ("Measure", "Column"):
            ref = node.get(obj_type)
            if isinstance(ref, dict) and "Property" in ref:
                return (dig(ref, "Expression", "SourceRef", "Entity"), ref.get("Property"), obj_type)
        for value in node.values():
            found = find_field(value)
            if found:
                return found
    elif isinstance(node, list):
        for item in node:
            found = find_field(item)
            if found:
                return found
    return None


def resolve_filter_field(expr):
    if not isinstance(expr, dict):
        return (None, None, None)
    hier_level = expr.get("HierarchyLevel")
    if isinstance(hier_level, dict):
        level = dig(hier_level, "Level")
        hier = dig(hier_level, "Expression", "Hierarchy", "Hierarchy")
        entity = dig(hier_level, "Expression", "Hierarchy", "Expression", "SourceRef", "Entity")
        if hier is not None and level is not None:
            return (entity, f"{hier}.{level}", "Hierarchy")
    return find_field(expr) or (None, None, None)


def filter_how_created(filter_type):
    if filter_type == "Advanced":
        return "Manual"
    if filter_type:
        return "Auto"
    return ""


_LIVE_TYPES = ("pbiServiceLive", "pbiServiceXmlaStyleLive", "analysisServicesDatabaseLive")


def _extract_server_name(connection_string):
    if not connection_string:
        return ""
    cs_lower = connection_string.lower()
    if "data source=" not in cs_lower or "initial catalog=" not in cs_lower:
        return ""
    start = cs_lower.index("data source=") + len("data source=")
    semi = cs_lower.find(";", start)
    if semi == -1:
        semi = len(connection_string)
    return connection_string[start:semi] if semi > start else ""


def parse_connections(conn_json_str, report_id_override=None, model_id_override=None):
    server_name = model_id = report_id = conn_type = ""
    conn = None
    if conn_json_str:
        if isinstance(conn_json_str, (dict, list)):
            conn = conn_json_str
        elif isinstance(conn_json_str, (bytes, bytearray)):
            try:
                conn = json.loads(bytes(conn_json_str).decode("utf-8-sig"))
            except Exception:
                conn = None
        elif isinstance(conn_json_str, str) and conn_json_str.strip():
            try:
                conn = json.loads(conn_json_str.lstrip("\ufeff"))
            except Exception:
                conn = None
    has_conn = isinstance(conn, dict)
    if has_conn:
        try:
            for o in (dig(conn, "Connections") or []):
                conn_type = digs(o, "ConnectionType") or conn_type
                if conn_type in _LIVE_TYPES:
                    model_id = digs(conn, "RemoteArtifacts", 0, "DatasetId") or model_id
                    report_id = digs(conn, "RemoteArtifacts", 0, "ReportId") or report_id
                else:
                    model_id = digs(o, "PbiModelDatabaseName") or model_id
                server_name = _extract_server_name(digs(o, "ConnectionString")) or server_name
        except Exception:
            model_id = digs(conn, "RemoteArtifacts", 0, "DatasetId") or model_id
            report_id = digs(conn, "RemoteArtifacts", 0, "ReportId") or report_id
            conn_type = "localPowerQuery"
    if report_id_override:
        report_id = report_id_override
    if model_id_override:
        model_id = model_id_override
    return {"server_name": server_name or "", "model_id": model_id or "",
            "report_id": report_id or "", "conn_type": conn_type or "", "has_conn": has_conn}


SELECT_CANDIDATES = [
    ("Column",  ["Column", "Property"], ["Column", "Expression", "SourceRef", "Source"]),
    ("Measure", ["Measure", "Property"], ["Measure", "Expression", "SourceRef", "Source"]),
    ("Column",  ["Arithmetic", "Left", "Aggregation", "Expression", "Column", "Property"],  ["Arithmetic", "Left", "Aggregation", "Expression", "Column", "Expression", "SourceRef", "Source"]),
    ("Column",  ["Arithmetic", "Right", "Aggregation", "Expression", "Column", "Property"], ["Arithmetic", "Right", "Aggregation", "Expression", "Column", "Expression", "SourceRef", "Source"]),
    ("Measure", ["Arithmetic", "Left", "Aggregation", "Expression", "Measure", "Property"], ["Arithmetic", "Left", "Aggregation", "Expression", "Measure", "Expression", "SourceRef", "Source"]),
    ("Measure", ["Arithmetic", "Right", "Aggregation", "Expression", "Measure", "Property"], ["Arithmetic", "Right", "Aggregation", "Expression", "Measure", "Expression", "SourceRef", "Source"]),
    ("Measure", ["Arithmetic", "Right", "Measure", "Property"], ["Arithmetic", "Right", "Measure", "Expression", "SourceRef", "Source"]),
    ("Column",  ["Arithmetic", "Left", "Column", "Property"],  ["Arithmetic", "Left", "Column", "Expression", "SourceRef", "Source"]),
    ("Measure", ["Arithmetic", "Left", "Measure", "Property"], ["Arithmetic", "Left", "Measure", "Expression", "SourceRef", "Source"]),
    ("Column",  ["Arithmetic", "Right", "Column", "Property"], ["Arithmetic", "Right", "Column", "Expression", "SourceRef", "Source"]),
    ("Column",  ["Aggregation", "Expression", "Column", "Property"],  ["Aggregation", "Expression", "Column", "Expression", "SourceRef", "Source"]),
    ("Measure", ["Aggregation", "Expression", "Measure", "Property"], ["Aggregation", "Expression", "Measure", "Expression", "SourceRef", "Source"]),
]

_COLOR_SPECS = [
    (("singleVisual", "objects", "labels"), "color", "Label"),
    (("singleVisual", "objects", "categoryAxis"), "labelColor", "X Axis Color"),
    (("singleVisual", "objects", "categoryAxis"), "titleColor", "X Axis Title Color"),
    (("singleVisual", "objects", "valueAxis"), "labelColor", "Y Axis Color"),
    (("singleVisual", "objects", "valueAxis"), "titleColor", "Y Axis Title Color"),
    (("singleVisual", "objects", "categoryLabels"), "color", "Category Label"),
    (("singleVisual", "objects", "values"), "backColor", "Conditional Formatting (Background Color)"),
    (("singleVisual", "objects", "values"), "fontColor", "Conditional Formatting (Font Color)"),
    (("singleVisual", "vcObjects", "title"), "fontColor", "Title Font Color"),
    (("singleVisual", "vcObjects", "title"), "background", "Title Background"),
    (("singleVisual", "vcObjects", "background"), "color", "Background"),
    (("singleVisual", "vcObjects", "border"), "color", "Border"),
    (("singleVisual", "vcObjects", "dropShadow"), "color", "Drop Shadow"),
]

OBJECT_EXTRACTORS = [
    (c, ("properties", p, "solid", "color", "expr"), s) for (c, p, s) in _COLOR_SPECS
] + [
    (("singleVisual", "objects", "text"), ("properties", "text", "expr"), "Text"),
    (("singleVisual", "objects", "values"), ("properties", "icon", "value", "expr"), "Conditional Formatting (Icon)"),
    (("singleVisual", "objects", "values"), ("properties", "webURL", "expr"), "Conditional Formatting (WebURL)"),
    (("singleVisual", "objects", "categoryAxis"), ("properties", "start", "expr"), "Y Axis Minimum"),
    (("singleVisual", "objects", "categoryAxis"), ("properties", "end", "expr"), "Y Axis Minimum"),
    (("singleVisual", "vcObjects", "title"), ("properties", "text", "expr"), "Title Text"),
    (("singleVisual", "vcObjects", "title"), ("properties", "text", "solid", "color", "expr"), "Title Text"),
]


def _table_resolver(from_list):
    alias = {}
    for t in (from_list or []):
        name = digs(t, "Name")
        if name is not None:
            alias[name] = digs(t, "Entity") or ""
    return lambda src: alias.get(src, "") if src is not None else ""


def _vo_row(table, obj, obj_type, source, display_name, applied_filter_version):
    return {"TableName": table or "", "ObjectName": obj, "ObjectType": obj_type,
            "Source": source, "displayName": display_name, "AppliedFilterVersion": applied_filter_version}


def extract_visual_objects(config, applied_filter_version):
    rows = []
    select = dig(config, "singleVisual", "prototypeQuery", "Select")
    if isinstance(select, list):
        resolve = _table_resolver(dig(config, "singleVisual", "prototypeQuery", "From"))
        for sel in select:
            object_name, object_type, src, is_spark = "", "", None, False
            for obj_type, name_path, src_path in SELECT_CANDIDATES:
                if dig(sel, *name_path) is not None:
                    object_name = digs(sel, *name_path)
                    object_type = obj_type
                    src = digs(sel, *src_path)
            level = dig(sel, "HierarchyLevel", "Level")
            if level is not None:
                hier = digs(sel, "HierarchyLevel", "Expression", "Hierarchy", "Hierarchy")
                object_name = f"{hier}.{digs(sel, 'HierarchyLevel', 'Level')}"
                object_type = "Hierarchy"
                src = digs(sel, "HierarchyLevel", "Expression", "Hierarchy", "Expression", "SourceRef", "Source")
            if dig(sel, "SparklineData", "Measure", "Measure", "Property") is not None:
                object_name = digs(sel, "SparklineData", "Measure", "Measure", "Property")
                object_type = "Measure"
                is_spark = True
                src = digs(sel, "SparklineData", "Measure", "Measure", "Expression", "SourceRef", "Source")
            if dig(sel, "SparklineData", "Measure", "Aggregation", "Expression", "Column", "Property") is not None:
                object_name = digs(sel, "SparklineData", "Measure", "Aggregation", "Expression", "Column", "Property")
                object_type = "Column"
                is_spark = True
                src = digs(sel, "SparklineData", "Measure", "Aggregation", "Expression", "Column", "Expression", "SourceRef", "Source")
            display_name = digs(config, "singleVisual", "columnProperties", digs(sel, "Name"), "displayName")
            source_label = "Sparkline" if is_spark else "Standard"
            rows.append(_vo_row(resolve(src), object_name, object_type, source_label, display_name, applied_filter_version))
            if is_spark and dig(sel, "SparklineData", "Groupings", 0, "Column", "Property") is not None:
                g_obj = digs(sel, "SparklineData", "Groupings", 0, "Column", "Property")
                g_src = digs(sel, "SparklineData", "Groupings", 0, "Column", "Expression", "SourceRef", "Source")
                rows.append(_vo_row(resolve(g_src), g_obj, "Column", source_label, display_name, None))
    for container_path, expr_subpath, source in OBJECT_EXTRACTORS:
        container = dig(config, *container_path)
        if not isinstance(container, list):
            continue
        for child in container:
            expr = dig(child, *expr_subpath)
            if expr is None:
                continue
            found = find_field(expr)
            if not found:
                continue
            entity, prop, obj_type = found
            rows.append(_vo_row(entity, prop, obj_type, source, digs(child, "displayName"), None))
    return rows


_INTERACTION_TYPES = ["blank", "Filter", "Highlight", "None"]


def _filter_fields(o):
    filter_type = digs(o, "type")
    table, obj, obj_type = resolve_filter_field(dig(o, "expression"))
    return {"displayName": digs(o, "displayName"), "TableName": table or "",
            "ObjectName": obj if obj is not None else "", "ObjectType": obj_type or "",
            "FilterType": filter_type, "HiddenFilter": digs(o, "isHiddenInViewMode"),
            "LockedFilter": digs(o, "isLockedInViewMode"), "HowCreated": filter_how_created(filter_type),
            "Used": "False", "AppliedFilterVersion": digs(o, "filter", "Version")}


def _page_type(display_opt, width, height):
    if display_opt == 3 and width == 320 and height == 240:
        return "Tooltip"
    if width == 816 and height == 1056:
        return "Letter"
    if width == 960 and height == 720:
        return "4:3"
    if width == 1280 and height == 720:
        return "16:9"
    return "Custom"


def _clean_dax(s):
    return (s or "").replace("\t", " ").replace("\r\n", " ").replace("\n", " ")


def parse_layout(layout, report_name):
    out = {k: [] for k in ("CustomVisuals", "ReportFilters", "PageFilters", "VisualFilters",
                           "VisualObjects", "Visuals", "Bookmarks", "Pages", "VisualInteractions",
                           "ReportLevelMeasures")}
    custom_visual_names = set()
    for o in (dig(layout, "resourcePackages") or []):
        if dig(o, "resourcePackage", "type") == 0:
            name = digs(o, "resourcePackage", "name")
            out["CustomVisuals"].append({"Name": name})
            if name:
                custom_visual_names.add(name)
    for o in (parse_json_field(layout, "filters") or []):
        out["ReportFilters"].append(_filter_fields(o))
    config = parse_json_field(layout, "config")

    def process_entities(entities):
        for ent in (entities or []):
            table_name = digs(ent, "name") or digs(ent, "Name")
            measures = dig(ent, "measures")
            if measures is None:
                measures = dig(ent, "Measures")
            for m in (measures or []):
                expr = digs(m, "expression") or digs(m, "Expression") or ""
                fmt = digs(m, "formatInformation", "formatString") or ""
                out["ReportLevelMeasures"].append({"TableName": table_name,
                    "ObjectName": digs(m, "name") or digs(m, "Name"), "ObjectType": "Measure",
                    "Expression": _clean_dax(expr), "HiddenFlag": "true" if as_bool(dig(m, "hidden")) else "false",
                    "FormatString": _clean_dax(fmt)})

    if isinstance(config, dict):
        model_ext = dig(config, "modelExtensions")
        if model_ext is None:
            model_ext = dig(config, "ModelExtensions")
        for me in (model_ext or []):
            ents = dig(me, "entities")
            if ents is None:
                ents = dig(me, "Entities")
            process_entities(ents)
        ext = dig(config, "Extension")
        if ext is not None:
            process_entities(dig(ext, "Entities"))

    pages_by_id = {}
    for o in (dig(layout, "sections") or []):
        page_id = digs(o, "name")
        page_name = digs(o, "displayName")
        if not page_name or not page_name.strip():
            page_name = report_name
        page_config = parse_json_field(o, "config")
        page_flt = parse_json_field(o, "filters")
        containers = dig(o, "visualContainers") or []
        width = to_int(dig(o, "width"))
        height = to_int(dig(o, "height"))
        display_opt = to_int(dig(o, "displayOption"))
        out["Pages"].append({"Id": page_id, "Name": page_name, "Number": to_int(dig(o, "ordinal"), 0),
            "Width": width, "Height": height, "DisplayOption": str(display_opt),
            "HiddenFlag": dig(page_config, "visibility") == 1, "VisualCount": len(containers),
            "DataVisualCount": 0, "VisibleVisualCount": 0,
            "PageFilterCount": len(page_flt) if isinstance(page_flt, list) else 0,
            "Type": _page_type(display_opt, width, height)})
        pages_by_id[page_id] = page_name
        for rel in (dig(page_config, "relationships") or []):
            tid = to_int(dig(rel, "type"), -1)
            out["VisualInteractions"].append({"PageName": page_name, "PageId": page_id,
                "SourceVisualID": digs(rel, "source"), "SourceVisualName": "",
                "TargetVisualID": digs(rel, "target"), "TargetVisualName": "", "TypeID": tid,
                "Type": _INTERACTION_TYPES[tid] if 0 <= tid < len(_INTERACTION_TYPES) else ""})
        for o2 in (page_flt or []):
            row = _filter_fields(o2)
            row["PageId"] = page_id
            row["PageName"] = page_name
            out["PageFilters"].append(row)
        for vc in containers:
            config_v = parse_json_field(vc, "config")
            visual_id = digs(config_v, "name")
            visual_type = digs(config_v, "singleVisual", "visualType") or "visualGroup"
            custom_flag = visual_type in custom_visual_names
            visual_name = digs(config_v, "singleVisualGroup", "displayName") or ""
            literal = digs(config_v, "singleVisual", "vcObjects", "title", 0, "properties", "text", "expr", "Literal", "Value")
            if literal is not None and len(literal) >= 2:
                visual_name = literal[1:-1]
            if not visual_name:
                visual_name = visual_type
            vis_hidden = (digs(config_v, "singleVisual", "display", "mode") == "hidden") or as_bool(dig(config_v, "singleVisualGroup", "isHidden"))
            applied_fv = digs(config_v, "singleVisual", "objects", "general", 0, "properties", "filter", "filter", "Version")
            select = dig(config_v, "singleVisual", "prototypeQuery", "Select")
            obj_count = len(select) if isinstance(select, list) else 0
            for vo in extract_visual_objects(config_v, applied_fv):
                out["VisualObjects"].append({"PageName": page_name, "PageId": page_id, "VisualId": visual_id,
                    "VisualName": visual_name, "VisualType": visual_type, "AppliedFilterVersion": vo["AppliedFilterVersion"],
                    "CustomVisualFlag": custom_flag, "TableName": vo["TableName"], "ObjectName": vo["ObjectName"],
                    "ObjectType": vo["ObjectType"], "ImplicitMeasure": False, "Sparkline": False,
                    "VisualCalc": False, "Format": "", "Source": vo["Source"], "displayName": vo["displayName"]})
            out["Visuals"].append({"PageName": page_name, "PageId": page_id, "Id": visual_id, "Name": visual_name,
                "Type": visual_type, "DisplayType": visual_type, "Title": "", "SubTitle": "", "AltText": "",
                "CustomVisualFlag": custom_flag, "HiddenFlag": vis_hidden, "X": to_int(dig(vc, "x")),
                "Y": to_int(dig(vc, "y")), "Z": to_int(dig(vc, "z")), "Width": to_int(dig(vc, "width")),
                "Height": to_int(dig(vc, "height")), "ObjectCount": obj_count, "VisualFilterCount": 0,
                "DataLimit": 0, "RowSubTotals": False, "ColumnSubTotals": False, "DataVisual": False,
                "HasSparkline": False, "ParentGroup": digs(config_v, "parentGroupName") or ""})
            for o3 in (parse_json_field(vc, "filters") or []):
                row = _filter_fields(o3)
                row["PageName"] = page_name
                row["PageId"] = page_id
                row["VisualId"] = visual_id
                row["VisualName"] = visual_name
                out["VisualFilters"].append(row)

    coords = {v["Id"]: (v["X"], v["Y"]) for v in out["Visuals"]}
    for v in out["Visuals"]:
        parent = v["ParentGroup"]
        if parent and parent in coords:
            px, py = coords[parent]
            v["X"] += px
            v["Y"] += py

    def add_leaf(node):
        if dig(node, "explorationState") is None:
            return
        page_id = digs(node, "explorationState", "activeSection")
        out["Bookmarks"].append({"Name": digs(node, "displayName"), "Id": digs(node, "name"),
            "PageName": pages_by_id.get(page_id, ""), "PageId": page_id, "VisualId": "",
            "VisualHiddenFlag": False, "SuppressData": False, "CurrentPageSelected": False,
            "ApplyVisualDisplayState": False, "ApplyToAllVisuals": False})

    if isinstance(config, dict):
        for bk in (dig(config, "bookmarks") or []):
            children = dig(bk, "children")
            if children is not None:
                for child in children:
                    add_leaf(child)
            else:
                add_leaf(bk)
    return out


def _get_report_definition_parts(ws_id, rpt_id):
    """Fetch a report's definition via the Fabric getDefinition API and return
    {part_path: bytes}. Mirrors the PowerShell Export-ReportDefinitionAsPbix
    staging (handles 200 and 202 long-running). Returns None on any failure so
    the caller can fall back to the ReportWrapper path."""
    if not ws_id or not rpt_id:
        return None
    try:
        client = FabricRestClient()
        url = f"v1/workspaces/{ws_id}/reports/{rpt_id}/getDefinition"
        resp = client.post(url, json={})
        status = getattr(resp, "status_code", None)
        body = None
        if status == 200:
            body = resp.json()
        elif status == 202:
            op_url = resp.headers.get("Location") or resp.headers.get("location")
            op_rel = op_url
            if op_url and op_url.lower().startswith("http"):
                idx = op_url.find("/v1/")
                op_rel = op_url[idx + 1:] if idx != -1 else op_url
            try:
                retry = int(resp.headers.get("Retry-After", "3") or "3")
            except Exception:
                retry = 3
            for _ in range(120):  # cap polling iterations
                time.sleep(max(1, retry))
                op = client.get(op_rel)
                st = (op.json() or {}).get("status")
                if st == "Succeeded":
                    break
                if st == "Failed":
                    return None
                try:
                    retry = int(op.headers.get("Retry-After", str(retry)) or retry)
                except Exception:
                    pass
            res = client.get(op_rel.rstrip("/") + "/result")
            body = res.json()
        else:
            return None
        parts = ((body or {}).get("definition") or {}).get("parts") or []
        decoded = {}
        for p in parts:
            path = p.get("path", "")
            payload = p.get("payload", "")
            if p.get("payloadType") == "InlineBase64":
                try:
                    decoded[path] = base64.b64decode(payload)
                except Exception:
                    decoded[path] = b""
            else:
                decoded[path] = (payload or "").encode("utf-8")
        return decoded
    except Exception:
        return None


def _is_classic_layout(parts):
    """Classic (non-PBIR) reports return a root-level 'report.json' (the legacy
    Report/Layout). PBIR reports place everything under a 'definition/' folder."""
    return "report.json" in parts and not any(p.startswith("definition/") for p in parts)


def _synth_connections_from_pbir(pbir_raw, rpt_id):
    """Build ServerName/ModelID/Type from definition.pbir's datasetReference,
    exactly like the PowerShell synthesized Connections file."""
    info = {"server_name": "", "model_id": "", "conn_type": ""}
    if not pbir_raw:
        return info
    try:
        pbir = json.loads(bytes(pbir_raw).decode("utf-8-sig"))
    except Exception:
        return info
    ds = pbir.get("datasetReference", {}) or {}
    conn_string, dataset_id = None, ""
    by_conn, by_path = ds.get("byConnection"), ds.get("byPath")
    if by_conn and by_conn.get("connectionString"):
        conn_string = by_conn.get("connectionString")
        m = re.search(r"semanticmodelid=([0-9a-fA-F-]+)", conn_string)
        if m:
            dataset_id = m.group(1)
    elif by_path and by_path.get("path"):
        conn_string = "byPath:" + by_path.get("path")
    if conn_string:
        conn_obj = {"Version": "3.0",
                    "Connections": [{"Name": "EntityDataSource", "ConnectionString": conn_string, "ConnectionType": "pbiServiceLive"}],
                    "RemoteArtifacts": [{"DatasetId": dataset_id, "ReportId": rpt_id}]}
        parsed = parse_connections(json.dumps(conn_obj))
        info.update(server_name=parsed["server_name"], model_id=parsed["model_id"], conn_type=parsed["conn_type"])
    return info


def _classic_bool(v):
    if isinstance(v, bool):
        return "True" if v else "False"
    if v is None:
        return "False"
    return "True" if str(v).strip().lower() in ("true", "1") else "False"


def extract_classic_report_metadata(parts, ws_name, rpt_name, rpt_id, model_id, report_date, result):
    """Parse a classic (non-PBIR) report definition into the same `result`
    structure ReportWrapper produces, mapped to the lakehouse table schemas."""
    raw = parts.get("report.json")
    layout = None
    if raw is not None:
        for enc in ("utf-8-sig", "utf-16-le", "utf-8"):
            try:
                layout = json.loads(bytes(raw).decode(enc))
                break
            except Exception:
                layout = None
    if not isinstance(layout, dict):
        result["error"] = "Classic report.json missing or invalid"
        return

    conn = _synth_connections_from_pbir(parts.get("definition.pbir"), rpt_id)
    mid = model_id or conn.get("model_id", "")
    out = parse_layout(layout, rpt_name)

    result["connections"].append({"ReportID": rpt_id, "ModelID": mid, "ReportDate": report_date,
        "ReportName": rpt_name, "Type": conn.get("conn_type", ""), "ServerName": conn.get("server_name", ""),
        "WorkspaceName": ws_name})

    base = {"ReportName": rpt_name, "ReportID": rpt_id, "ModelID": mid, "ReportDate": report_date, "WorkspaceName": ws_name}

    for p in out["Pages"]:
        result["pages"].append({**base, "Id": p["Id"], "Name": p["Name"], "Number": int(p["Number"]),
            "Width": int(p["Width"]), "Height": int(p["Height"]), "HiddenFlag": _classic_bool(p["HiddenFlag"]),
            "VisualCount": int(p["VisualCount"]), "Type": p["Type"], "DisplayOption": str(p["DisplayOption"]),
            "DataVisualCount": int(p["DataVisualCount"]), "VisibleVisualCount": int(p["VisibleVisualCount"]),
            "PageFilterCount": int(p["PageFilterCount"])})
    for v in out["Visuals"]:
        result["visuals"].append({**base, "PageName": v["PageName"], "PageId": v["PageId"], "Id": v["Id"],
            "Name": v["Name"], "Type": v["Type"], "DisplayType": v["DisplayType"], "Title": v["Title"],
            "SubTitle": v["SubTitle"], "AltText": v["AltText"], "TabOrder": 0, "CustomVisualFlag": _classic_bool(v["CustomVisualFlag"]),
            "HiddenFlag": _classic_bool(v["HiddenFlag"]), "X": float(v["X"]), "Y": float(v["Y"]), "Z": int(v["Z"]),
            "Width": float(v["Width"]), "Height": float(v["Height"]), "ObjectCount": int(v["ObjectCount"]),
            "VisualFilterCount": int(v["VisualFilterCount"]), "DataLimit": int(v["DataLimit"]),
            "Divider": "False", "RowSubTotals": _classic_bool(v["RowSubTotals"]), "ColumnSubTotals": _classic_bool(v["ColumnSubTotals"]),
            "DataVisual": _classic_bool(v["DataVisual"]), "HasSparkline": _classic_bool(v["HasSparkline"]), "ParentGroup": v["ParentGroup"]})
    for b in out["Bookmarks"]:
        result["bookmarks"].append({**base, "Name": b["Name"], "Id": b["Id"], "PageName": b["PageName"],
            "PageId": b["PageId"], "VisualId": b["VisualId"], "VisualHiddenFlag": _classic_bool(b["VisualHiddenFlag"]),
            "SuppressData": _classic_bool(b["SuppressData"]), "CurrentPageSelected": _classic_bool(b["CurrentPageSelected"]),
            "ApplyVisualDisplayState": _classic_bool(b["ApplyVisualDisplayState"]), "ApplyToAllVisuals": _classic_bool(b["ApplyToAllVisuals"])})
    for c in out["CustomVisuals"]:
        result["custom_visuals"].append({**base, "Name": c["Name"]})
    for f in out["ReportFilters"]:
        result["report_filters"].append({**base, "displayName": f["displayName"], "TableName": f["TableName"],
            "ObjectName": f["ObjectName"], "ObjectType": f["ObjectType"], "FilterType": f["FilterType"],
            "HiddenFilter": _classic_bool(f["HiddenFilter"]), "LockedFilter": _classic_bool(f["LockedFilter"]),
            "HowCreated": f["HowCreated"], "Used": f["Used"]})
    for f in out["PageFilters"]:
        result["page_filters"].append({**base, "PageId": f["PageId"], "PageName": f["PageName"],
            "displayName": f["displayName"], "TableName": f["TableName"], "ObjectName": f["ObjectName"],
            "ObjectType": f["ObjectType"], "FilterType": f["FilterType"], "HiddenFilter": _classic_bool(f["HiddenFilter"]),
            "LockedFilter": _classic_bool(f["LockedFilter"]), "HowCreated": f["HowCreated"], "Used": f["Used"]})
    for f in out["VisualFilters"]:
        result["visual_filters"].append({**base, "PageName": f["PageName"], "PageId": f["PageId"], "VisualId": f["VisualId"],
            "TableName": f["TableName"], "ObjectName": f["ObjectName"], "ObjectType": f["ObjectType"],
            "FilterType": f["FilterType"], "HiddenFilter": _classic_bool(f["HiddenFilter"]), "LockedFilter": _classic_bool(f["LockedFilter"]),
            "displayName": f["displayName"], "HowCreated": f["HowCreated"], "Used": f["Used"]})
    for v in out["VisualObjects"]:
        result["visual_objects"].append({**base, "PageName": v["PageName"], "PageId": v["PageId"], "VisualId": v["VisualId"],
            "VisualName": v["VisualName"], "VisualType": v["VisualType"], "CustomVisualFlag": _classic_bool(v["CustomVisualFlag"]),
            "TableName": v["TableName"], "ObjectName": v["ObjectName"], "ObjectType": v["ObjectType"], "Source": v["Source"],
            "displayName": v["displayName"], "ImplicitMeasure": _classic_bool(v["ImplicitMeasure"]), "Sparkline": _classic_bool(v["Sparkline"]),
            "VisualCalc": _classic_bool(v["VisualCalc"]), "Format": v["Format"]})
    for m in out["ReportLevelMeasures"]:
        result["report_level_measures"].append({**base, "TableName": m["TableName"], "ObjectName": m["ObjectName"],
            "ObjectType": m["ObjectType"], "Expression": m["Expression"], "HiddenFlag": _classic_bool(m["HiddenFlag"]),
            "FormatString": m["FormatString"], "DataType": "", "DataCategory": ""})
    for vi in out["VisualInteractions"]:
        result["visual_interactions"].append({**base, "PageName": vi["PageName"], "PageId": vi["PageId"],
            "SourceVisualID": vi["SourceVisualID"], "TargetVisualID": vi["TargetVisualID"],
            "SourceVisualName": vi["SourceVisualName"], "TargetVisualName": vi["TargetVisualName"],
            "TypeID": str(vi["TypeID"]), "Type": vi["Type"]})


# ==============================================================  
# PARALLEL REPORT EXTRACTION HELPER
# ==============================================================

def extract_report_metadata(ws_name, ws_id, rpt_name, rpt_id, model_id, report_date):
    """Extract metadata for a single report.

    ReportWrapper is the primary path and handles PBIR reports. It internally
    reads the report definition (the same getDefinition API) and raises for
    classic (non-PBIR) reports - typically an "only supported on PBIR" style
    error. When that happens we fall back to fetching the definition ourselves
    and parsing the legacy 'report.json' with the embedded classic parser.

    The fallback is gated by _is_classic_layout(), so a genuine PBIR failure for
    any other reason is preserved as the original error rather than being
    mis-parsed. Doing it this way avoids a redundant getDefinition call for the
    common PBIR case (ReportWrapper already makes it)."""
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
        # ReportWrapper failed. For a classic (non-PBIR) report this is the
        # expected signal ("...only supported on PBIR..."). Fetch the report
        # definition via the Fabric getDefinition API and, if it is the legacy
        # layout, parse it with the embedded classic parser. _is_classic_layout
        # gates this, so a genuine PBIR failure for any other reason is kept as
        # the original error instead of being mis-handled.
        parts = None
        try:
            parts = _get_report_definition_parts(ws_id, rpt_id)
        except Exception:
            parts = None

        if parts is not None and _is_classic_layout(parts):
            # Discard any partial PBIR data captured before the failure.
            for _k in result:
                if _k != 'error':
                    result[_k] = []
            result['error'] = None
            try:
                extract_classic_report_metadata(parts, ws_name, rpt_name, rpt_id, model_id, report_date, result)
            except Exception as ce:
                result['error'] = f"Classic parse failed: {ce}"
        else:
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
all_report_tasks = []  # (ws_name, ws_id, rpt_name, rpt_id, model_id)

if not EXTRACT_REPORT_METADATA:
    log("\nSkipping report metadata extraction (EXTRACT_REPORT_METADATA=False)")
    log("Empty tables will be created with correct schema.")
else:
    for ws_row in workspaces_df.itertuples(index=False):
        ws_name = ws_row.Name
        ws_id = getattr(ws_row, "Id", None)
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
                
                all_report_tasks.append((ws_name, ws_id, rpt_name, rpt_id, model_id))

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
            executor.submit(extract_report_metadata, ws_name, ws_id, rpt_name, rpt_id, model_id, REPORT_DATE): (ws_name, rpt_name)
            for ws_name, ws_id, rpt_name, rpt_id, model_id in all_report_tasks
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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
dataflow_extract_tasks = []  # (gen, ws_id, df_id, df_name, ws_name)

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
        log(f"No data for {name}, creating empty table with schema")
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
log("DATAFLOW DETAIL EXTRACTION COMPLETE")
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
