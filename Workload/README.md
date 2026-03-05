# ImpactIQ Governance Fabric Workload

This directory contains the Microsoft Fabric Workload implementation for ImpactIQ Semantic Link Labs, enabling one-click deployment of the Power BI and Fabric governance solution into any organization's Fabric environment.

## 🎯 What is This?

This is a **Fabric Workload** built using the [Microsoft Fabric Extensibility Toolkit](https://github.com/microsoft/fabric-extensibility-toolkit). It packages the ImpactIQ governance solution as a native Fabric experience, allowing organizations to:

- **One-Click Deploy**: Install ImpactIQ directly from the Fabric Workload Hub
- **Native Integration**: Work seamlessly within the Fabric interface
- **Custom Item Types**: Create "Governance Analyzer" items in any workspace
- **Automated Governance**: Schedule and run governance analysis as part of Fabric pipelines

## 📦 What's Included

### Workload Components

```
Workload/
├── Manifest/                      # Workload definition and metadata
│   ├── WorkloadManifest.xml      # Main workload configuration
│   ├── Product.json              # Product details and UI configuration
│   ├── items/                    # Item type definitions
│   │   └── GovernanceAnalyzerItem/
│   │       ├── GovernanceAnalyzerItem.xml
│   │       └── GovernanceAnalyzerItem.json
│   └── assets/                   # Images and localization
│       ├── images/               # Icons and banners
│       └── locales/              # Translations
├── app/                          # Frontend application
│   ├── items/                    # Item editor components
│   │   └── GovernanceAnalyzerItem/
│   │       └── GovernanceAnalyzerItemEditor.tsx
│   ├── App.tsx                   # Main application component
│   └── ...
├── scripts/                      # Setup and deployment scripts
│   ├── Setup/                    # Initial setup scripts
│   ├── Run/                      # Development runtime scripts
│   └── Build/                    # Build and packaging scripts
└── package.json                  # Node.js dependencies
```

## 🚀 Quick Start

### Prerequisites

Before you begin, ensure you have:

- **[Node.js](https://nodejs.org/)** (v16 or later)
- **[PowerShell 7](https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell)**
- **[.NET SDK](https://dotnet.microsoft.com/download)** (for MacOS: x64 version)
- **[Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)**
- **[VSCode](https://code.visualstudio.com/)** or similar IDE
- **Fabric Workspace** with a Fabric Capacity assigned
- **Entra App Registration** (Azure AD) for authentication

### Step 1: Clone and Setup

```bash
# Clone the repository
git clone https://github.com/BeSmarterWithData/ImpactIQ-SemanticLinkLabs.git
cd ImpactIQ-SemanticLinkLabs/Workload

# Install dependencies
npm install

# Run the setup script
cd ../scripts/Setup
pwsh ./Setup.ps1 -WorkloadName "Org.ImpactIQGovernance"
```

The setup script will:
1. Create or configure an Entra App for authentication
2. Generate environment configuration files
3. Set up the development environment
4. Configure the workload manifest

### Step 2: Start Development Environment

```bash
# Start the development server (Terminal 1)
cd ../scripts/Run
pwsh ./StartDevServer.ps1

# In a new terminal, start the dev gateway (Terminal 2)
pwsh ./StartDevGateway.ps1
```

### Step 3: Enable in Fabric

1. Navigate to [Fabric Admin Portal](https://app.fabric.microsoft.com/admin-portal)
2. Go to **Tenant Settings**
3. Enable **Developer Mode** for your organization
4. Navigate to **Developer Settings** in your profile
5. Enable **Fabric Developer Mode**

### Step 4: Test Your Workload

1. Go to `https://app.fabric.microsoft.com/workloadhub/detail/Org.ImpactIQGovernance.Product?experience=fabric-developer`
2. Click on **Governance Analyzer** item type
3. Select your development workspace
4. Create your first governance analyzer item!

## 🔧 Configuration

### Environment Variables

The workload uses environment-specific configuration files:

- `.env.dev` - Development environment
- `.env.test` - Testing environment  
- `.env.prod` - Production environment

Key variables:
```bash
WORKLOAD_NAME=Org.ImpactIQGovernance
WORKLOAD_VERSION=1.0.0
FRONTEND_APPID=<your-entra-app-id>
FRONTEND_URL=http://localhost:60006
```

### Customization

#### Modify Item Types

To customize the Governance Analyzer item:

1. Edit the item manifest: `Workload/Manifest/items/GovernanceAnalyzerItem/`
2. Modify the React component: `Workload/app/items/GovernanceAnalyzerItem/GovernanceAnalyzerItemEditor.tsx`
3. Update translations: `Workload/Manifest/assets/locales/en-US/translations.json`

#### Add New Item Types

```bash
cd scripts/Setup
pwsh ./CreateNewItem.ps1 -ItemName "MyNewItem"
```

## 📚 Integration with GovernanceNotebook

The workload integrates with the existing `GovernanceNotebook.py` to provide governance functionality:

1. **Item Creation**: When users create a Governance Analyzer item, it can trigger notebook execution
2. **Lakehouse Connection**: Items can be linked to specific Lakehouses for metadata storage
3. **Scheduled Execution**: Integrate with Fabric Pipelines for automated governance runs
4. **Results Visualization**: Display governance results directly in the item editor

To integrate:

```typescript
// In GovernanceAnalyzerItemEditor.tsx
const runGovernanceAnalysis = async () => {
  // Trigger notebook execution via Fabric APIs
  const result = await fabricClient.executeNotebook({
    notebookName: 'GovernanceNotebook',
    workspaceId: workspaceId,
    parameters: {
      lakehouseSchema: 'dbo',
      workspaceNames: ['All']
    }
  });
};
```

## 🏗️ Building for Production

### Build the Workload

```bash
cd Workload
npm run build:prod
```

This creates a production bundle in `build/Frontend/`.

### Package for Distribution

```bash
cd ../scripts/Build
pwsh ./BuildManifestPackage.ps1
```

This creates a NuGet package (.nupkg) containing the workload manifest and assets.

### Deploy to Fabric

```bash
cd ../scripts/Deploy
pwsh ./DeployWorkload.ps1 -Environment "Production"
```

## 📖 Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────┐
│           Fabric Workload Hub                       │
│  (Workload Discovery & Installation)                │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│         ImpactIQ Governance Workload                │
│  ┌────────────────────────────────────────────┐    │
│  │  Governance Analyzer Item Type             │    │
│  │  - Create/Edit items in workspaces         │    │
│  │  - Configure analysis parameters           │    │
│  │  - View governance results                 │    │
│  └────────────────┬───────────────────────────┘    │
│                   │                                  │
│  ┌────────────────▼───────────────────────────┐    │
│  │  Fabric API Integration                    │    │
│  │  - Notebook execution (GovernanceNotebook) │    │
│  │  - Lakehouse data access                   │    │
│  │  - Workspace management                    │    │
│  └────────────────┬───────────────────────────┘    │
└───────────────────┼──────────────────────────────────┘
                    │
┌───────────────────▼──────────────────────────────────┐
│          Fabric Lakehouse                            │
│  - Metadata storage (dbo schema)                     │
│  - Report/Model/Dataflow metadata                    │
│  - Impact analysis results                           │
└──────────────────────────────────────────────────────┘
```

### Technology Stack

- **Frontend**: React 18 + TypeScript
- **UI Framework**: Fluent UI React Components
- **Build Tool**: Webpack 5
- **State Management**: Redux Toolkit
- **Fabric SDK**: @ms-fabric/workload-client v3
- **Backend**: GovernanceNotebook.py (Fabric Notebook)

## 🔐 Security & Authentication

The workload uses **Entra (Azure AD) authentication**:

1. Users authenticate via their Microsoft/Entra account
2. The workload uses delegated permissions to access Fabric APIs
3. All API calls are made on behalf of the authenticated user
4. No credentials are stored in the workload

Required Entra App Permissions:
- `Workspace.Read.All` - Read workspace information
- `Item.Execute.All` - Execute notebooks and pipelines
- `Dataset.Read.All` - Read semantic model metadata

## 📊 Publishing to Workload Hub

To make your workload available to all Fabric users:

1. **Partner Registration**: Register as a Microsoft Partner
2. **Certification**: Complete Fabric workload certification
3. **Publishing**: Submit to Fabric Workload Hub
4. **Distribution**: Users can install from the hub

See [Microsoft Fabric Publishing Guide](https://learn.microsoft.com/fabric/extensibility-toolkit/publishing-overview) for details.

## 🐛 Troubleshooting

### Common Issues

**Issue**: Workload doesn't appear in Fabric
- **Solution**: Ensure Developer Mode is enabled in tenant settings and user settings

**Issue**: Authentication fails
- **Solution**: Verify Entra App redirect URIs include your development URL

**Issue**: Build errors
- **Solution**: Clear node_modules and reinstall: `rm -rf node_modules && npm install`

**Issue**: Dev gateway connection fails
- **Solution**: Check that both dev server and dev gateway are running

### Logs and Debugging

- **Frontend logs**: Browser console (F12)
- **Backend logs**: PowerShell terminal running StartDevServer.ps1
- **Fabric logs**: Fabric workspace monitoring

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly in dev mode
5. Submit a pull request

## 📝 License

This project inherits the license from the main ImpactIQ repository.

## 🔗 Resources

- [Fabric Extensibility Toolkit](https://github.com/microsoft/fabric-extensibility-toolkit)
- [Fabric Workload Samples](https://github.com/microsoft/Microsoft-Fabric-tools-workload)
- [Fabric Documentation](https://learn.microsoft.com/fabric/)
- [Main ImpactIQ Repository](https://github.com/BeSmarterWithData/ImpactIQ-SemanticLinkLabs)

## 💬 Support

- **Issues**: [GitHub Issues](https://github.com/BeSmarterWithData/ImpactIQ-SemanticLinkLabs/issues)
- **Discussions**: [GitHub Discussions](https://github.com/BeSmarterWithData/ImpactIQ-SemanticLinkLabs/discussions)
- **Main Repo**: [ImpactIQ-SemanticLinkLabs](https://github.com/BeSmarterWithData/ImpactIQ-SemanticLinkLabs)

---

**Built with ❤️ using the Microsoft Fabric Extensibility Toolkit**
