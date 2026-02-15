# ImpactIQ Fabric Workload - Architecture Overview

## System Architecture

```
┌───────────────────────────────────────────────────────────────────────────┐
│                         Microsoft Fabric Platform                          │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                     Fabric Workload Hub                              │ │
│  │  (Discovery, Installation, Management)                               │ │
│  └────────────────────────────┬────────────────────────────────────────┘ │
│                               │                                            │
│  ┌────────────────────────────▼────────────────────────────────────────┐ │
│  │              ImpactIQ Governance Workload                            │ │
│  │                                                                       │ │
│  │  ┌──────────────────────────────────────────────────────────────┐  │ │
│  │  │  Frontend (React + TypeScript + Fluent UI)                   │  │ │
│  │  │  ┌────────────────┐  ┌──────────────────┐  ┌──────────────┐ │  │ │
│  │  │  │ Item Editors   │  │ Governance Client│  │ UI Components │ │  │ │
│  │  │  │ - Analyzer     │  │ - API Wrapper    │  │ - Cards       │ │  │ │
│  │  │  │ - Settings     │  │ - State Mgmt     │  │ - Forms       │ │  │ │
│  │  │  └────────────────┘  └──────────────────┘  └──────────────┘ │  │ │
│  │  └──────────────────────────┬───────────────────────────────────┘  │ │
│  │                             │                                        │ │
│  │  ┌──────────────────────────▼───────────────────────────────────┐  │ │
│  │  │  Workload API Layer (Future)                                 │  │ │
│  │  │  - Notebook Execution API                                    │  │ │
│  │  │  - Results Retrieval API                                     │  │ │
│  │  │  - Configuration API                                         │  │ │
│  │  └──────────────────────────┬───────────────────────────────────┘  │ │
│  └────────────────────────────┼────────────────────────────────────────┘ │
│                               │                                            │
│  ┌────────────────────────────▼────────────────────────────────────────┐ │
│  │              Fabric Notebook Engine                                  │ │
│  │  ┌──────────────────────────────────────────────────────────────┐  │ │
│  │  │  GovernanceNotebook.py                                       │  │ │
│  │  │  - Semantic Link Labs Integration                           │  │ │
│  │  │  - Power BI API Client                                      │  │ │
│  │  │  - Metadata Extraction                                      │  │ │
│  │  │  - Impact Analysis Engine                                   │  │ │
│  │  │  - Parallel Processing (5 workers)                          │  │ │
│  │  └──────────────────────────┬───────────────────────────────────┘  │ │
│  └────────────────────────────┼────────────────────────────────────────┘ │
│                               │                                            │
│  ┌────────────────────────────▼────────────────────────────────────────┐ │
│  │              Fabric Lakehouse                                        │ │
│  │  ┌──────────────────────────────────────────────────────────────┐  │ │
│  │  │  Governance Metadata Schema (dbo)                           │  │ │
│  │  │  - Reports Table                                            │  │ │
│  │  │  - Models Table                                             │  │ │
│  │  │  - Dataflows Table                                          │  │ │
│  │  │  - Columns Table                                            │  │ │
│  │  │  - Measures Table                                           │  │ │
│  │  │  - Relationships Table                                      │  │ │
│  │  │  - Visuals Table                                            │  │ │
│  │  │  - Impact Analysis Results                                  │  │ │
│  │  └──────────────────────────────────────────────────────────────┘  │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│                                                                            │
└───────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────────┐
│                      External Integration Points                           │
│                                                                            │
│  ┌─────────────────────┐  ┌──────────────────────┐  ┌─────────────────┐ │
│  │ Power BI Service    │  │ Fabric Workspaces    │  │ Entra ID (Auth) │ │
│  │ - REST API          │  │ - Semantic Models    │  │ - OAuth 2.0     │ │
│  │ - Metadata          │  │ - Reports            │  │ - JWT Tokens    │ │
│  └─────────────────────┘  └──────────────────────┘  └─────────────────┘ │
└───────────────────────────────────────────────────────────────────────────┘
```

## Component Breakdown

### 1. Frontend Layer (React + TypeScript)

**Purpose:** Provides the user interface for the workload within Fabric.

**Key Components:**
- **App.tsx**: Main application entry point with routing
- **GovernanceAnalyzerItemEditor.tsx**: Primary item editor component
- **GovernanceClient.ts**: API client for backend communication
- **theme.tsx**: Fluent UI theme configuration
- **constants.ts**: Application-wide constants

**Technologies:**
- React 18 for UI components
- TypeScript for type safety
- Fluent UI React Components for Microsoft design system
- Redux Toolkit for state management
- React Router for navigation

**Responsibilities:**
- Render item editors within Fabric
- Handle user interactions
- Display governance results
- Manage application state
- Communicate with backend APIs

### 2. Workload Manifest

**Purpose:** Defines the workload structure and metadata for Fabric.

**Key Files:**
- **WorkloadManifest.xml**: Core workload definition
  - Workload name and version
  - Hosting configuration
  - Authentication setup
  - Endpoint definitions

- **Product.json**: UI configuration
  - Display names and descriptions
  - Item type definitions
  - Workload Hub presentation
  - Icons and assets

- **Item Manifests**: Define custom item types
  - GovernanceAnalyzerItem.xml: Item type registration
  - GovernanceAnalyzerItem.json: Item metadata

**Standards:**
- Schema version 2.0.0
- XML Schema validation
- NuGet packaging format

### 3. Backend Integration (Python)

**Purpose:** Execute governance analysis and extract metadata.

**Key Components:**
- **GovernanceNotebook.py**: Main analysis engine
  - Semantic Link Labs integration
  - Power BI metadata extraction
  - Impact analysis algorithms
  - Parallel processing
  - Lakehouse data writing

- **workload_integration.py**: Workload integration helper
  - API interface for triggering analysis
  - Results retrieval methods
  - Status checking
  - Configuration management

**Responsibilities:**
- Connect to Power BI/Fabric APIs
- Extract report and model metadata
- Perform impact analysis
- Identify unused objects
- Detect broken visuals
- Write results to Lakehouse

### 4. Data Storage (Fabric Lakehouse)

**Purpose:** Store and query governance metadata.

**Schema Structure:**
```sql
-- Reports metadata
CREATE TABLE dbo.Reports (
    ReportId STRING,
    ReportName STRING,
    WorkspaceName STRING,
    WorkspaceId STRING,
    CreatedDate TIMESTAMP,
    ModifiedDate TIMESTAMP,
    ...
)

-- Models metadata
CREATE TABLE dbo.Models (
    ModelId STRING,
    ModelName STRING,
    Tables ARRAY<STRUCT>,
    Measures ARRAY<STRUCT>,
    ...
)

-- Impact analysis results
CREATE TABLE dbo.ImpactAnalysis (
    ObjectName STRING,
    ObjectType STRING,
    ImpactedReports ARRAY<STRING>,
    ImpactedVisuals ARRAY<STRUCT>,
    ...
)
```

**Access Patterns:**
- SQL queries via Spark
- Direct table access from Power BI
- REST API for programmatic access

## Data Flow

### Analysis Workflow

```
1. User Action (Frontend)
   │
   ├─> Click "Start Analysis" button
   │
   ├─> GovernanceClient.triggerAnalysis()
   │
   └─> API Request
       │
2. Backend Processing (Notebook)
   │
   ├─> Receive configuration
   │   - Workspace selection
   │   - Lakehouse connection
   │   - Parallel workers
   │
   ├─> Initialize Semantic Link
   │
   ├─> Extract Metadata (Parallel)
   │   ├─> Workspace 1 → Reports → Models → Dataflows
   │   ├─> Workspace 2 → Reports → Models → Dataflows
   │   └─> Workspace N → Reports → Models → Dataflows
   │
   ├─> Perform Analysis
   │   ├─> Impact Analysis
   │   ├─> Usage Detection
   │   ├─> Broken Visual Detection
   │   └─> Unused Object Detection
   │
   └─> Write to Lakehouse
       │
3. Results Storage (Lakehouse)
   │
   ├─> Write metadata tables
   │
   ├─> Write analysis results
   │
   └─> Update timestamps
       │
4. Results Retrieval (Frontend)
   │
   ├─> Click "Refresh Results"
   │
   ├─> GovernanceClient.getResults()
   │
   ├─> Query Lakehouse via SQL
   │
   └─> Display in UI
```

### Authentication Flow

```
1. User Login
   │
   ├─> Navigate to Fabric
   │
   ├─> Authenticate with Entra ID
   │
   └─> Receive access token (JWT)
       │
2. Workload Access
   │
   ├─> Open Governance Analyzer item
   │
   ├─> Frontend requests resources
   │
   ├─> Include access token in requests
   │
   └─> Token validated by Fabric
       │
3. API Calls
   │
   ├─> Frontend calls backend API
   │
   ├─> Pass user context (delegated)
   │
   ├─> Backend uses user's permissions
   │
   └─> Access Power BI data as user
```

## Security Architecture

### Authentication & Authorization

**Entra ID Integration:**
- OAuth 2.0 authorization code flow
- Single-page application (SPA) configuration
- Delegated permissions (user context)

**Required Permissions:**
- `Workspace.Read.All`: Read workspace metadata
- `Item.Execute.All`: Execute notebooks
- `Dataset.Read.All`: Read semantic model metadata

**Security Best Practices:**
- No credentials stored in code
- All API calls use user's access token
- Secrets stored in Azure Key Vault (production)
- HTTPS only for production endpoints
- CORS configured for Fabric domains only

### Data Security

**In Transit:**
- HTTPS/TLS 1.2+ required
- Secure WebSocket connections
- Token encryption in headers

**At Rest:**
- Lakehouse data encrypted by Fabric
- No sensitive data in application state
- Session storage cleared on logout

**Access Control:**
- Row-level security in Lakehouse (optional)
- Workspace-based isolation
- User permissions respected

## Scalability Considerations

### Performance Optimization

**Frontend:**
- Code splitting for large components
- Lazy loading of item editors
- Virtualized lists for large datasets
- Caching of API responses
- Debounced user inputs

**Backend:**
- Parallel processing (configurable workers)
- Batch API calls where possible
- Incremental data updates
- Query optimization in Lakehouse
- Connection pooling

**Data Layer:**
- Partitioned tables by workspace/date
- Indexed columns for common queries
- Materialized views for aggregations
- Delta Lake for ACID transactions

### Scalability Limits

**Current Architecture:**
- Supports: 100s of workspaces
- Handles: 1000s of reports
- Processes: 10,000s of objects
- Concurrent users: 10s

**Future Enhancements for Scale:**
- Distributed processing with Spark
- Microservices architecture
- Message queue for async processing
- CDN for static assets
- Caching layer (Redis)

## Deployment Architecture

### Development Environment

```
Developer Machine
├── Node.js Runtime
│   └── Webpack Dev Server (port 60006)
│       └── Hot Module Replacement
├── PowerShell 7
│   └── Dev Gateway Script
│       └── Fabric Registration
└── Local Testing
    └── Browser → localhost:60006
```

### Production Environment (Option 1: Azure)

```
Azure App Service
├── Web App (Linux)
│   ├── Node.js 18
│   ├── Static Files (Frontend)
│   └── HTTPS Endpoint
├── Application Insights
│   └── Monitoring & Logging
└── Azure Front Door (Optional)
    └── CDN & WAF
```

### Production Environment (Option 2: Custom Hosting)

```
Custom Infrastructure
├── Web Server (nginx/IIS)
│   ├── Static File Serving
│   ├── HTTPS Configuration
│   └── Compression (gzip/brotli)
├── Load Balancer
│   └── Multi-region Support
└── Monitoring Stack
    ├── Application Logs
    ├── Performance Metrics
    └── Error Tracking
```

## Extension Points

### Future Enhancements

**Additional Item Types:**
- Compliance Checker Item
- Cost Optimizer Item
- Performance Analyzer Item
- Usage Dashboard Item

**API Enhancements:**
- Real-time analysis updates (WebSockets)
- Scheduled analysis configuration
- Automated alerts and notifications
- Export to various formats

**Integration Points:**
- Azure DevOps for CI/CD
- Teams for notifications
- Power Automate for workflows
- Email for reports

**Advanced Features:**
- Machine learning for anomaly detection
- Predictive impact analysis
- Automated optimization recommendations
- Custom rule engine

## Technology Stack Summary

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| Frontend Framework | React | 18.x | UI component library |
| Language | TypeScript | 5.x | Type-safe JavaScript |
| UI Components | Fluent UI | 9.x | Microsoft design system |
| State Management | Redux Toolkit | 2.x | Application state |
| Build Tool | Webpack | 5.x | Module bundling |
| Package Manager | npm | Latest | Dependency management |
| Backend | Python | 3.x | Data processing |
| Data Processing | Spark | Latest | Big data processing |
| Storage | Fabric Lakehouse | - | Metadata storage |
| Authentication | Entra ID | - | Identity & access |
| Hosting | Configurable | - | Web hosting |

## Maintenance & Operations

### Monitoring

**Key Metrics:**
- Frontend load time
- API response time
- Notebook execution time
- Error rates
- User adoption metrics

**Logging:**
- Application logs
- Audit logs (user actions)
- Performance logs
- Error logs with stack traces

**Alerts:**
- High error rate
- Slow performance
- Failed notebook executions
- Authentication failures

### Update Strategy

**Frontend Updates:**
1. Develop feature in branch
2. Test in dev environment
3. Build production bundle
4. Deploy to staging
5. Validate and promote to production

**Backend Updates:**
1. Update GovernanceNotebook.py
2. Test with sample data
3. Update workload_integration.py
4. Deploy to Fabric workspace
5. Trigger test run

**Manifest Updates:**
1. Update version in manifest
2. Rebuild NuGet package
3. Deploy to test tenant
4. Validate functionality
5. Deploy to production tenant

### Backup & Recovery

**Code:**
- Git repository with branches
- Tagged releases
- Pull request history

**Configuration:**
- Environment files backed up
- Entra app configuration documented
- Deployment scripts version controlled

**Data:**
- Lakehouse backups via Fabric
- Metadata export scripts
- Restore procedures documented

---

## Quick Reference

**Repository Structure:**
```
ImpactIQ-SemanticLinkLabs/
├── Workload/              # Fabric Workload implementation
│   ├── Manifest/          # Workload definition
│   ├── app/              # Frontend application
│   ├── scripts/          # Build & deploy scripts
│   └── devServer/        # Development server
├── GovernanceNotebook.py # Main analysis notebook
└── workload_integration.py # Integration helper
```

**Key Commands:**
```bash
# Development
npm install           # Install dependencies
npm run start         # Start dev server
pwsh ./Setup.ps1     # Initial setup

# Building
npm run build:prod    # Production build
pwsh ./BuildManifestPackage.ps1  # Create package

# Deployment
pwsh ./DeployWorkload.ps1  # Deploy to Fabric
```

**Important URLs:**
- Workload Hub: `https://app.fabric.microsoft.com/workloadhub`
- Admin Portal: `https://app.fabric.microsoft.com/admin-portal`
- Documentation: `./Workload/README.md`
- Deployment Guide: `./Workload/DEPLOYMENT.md`

---

**For detailed information on specific topics, refer to:**
- Setup: `Workload/README.md`
- Deployment: `Workload/DEPLOYMENT.md`
- Testing: `Workload/TESTING.md`
- GitHub: https://github.com/BeSmarterWithData/ImpactIQ-SemanticLinkLabs
