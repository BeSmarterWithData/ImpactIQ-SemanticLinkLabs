# Fabric Workload Deployment Guide

This guide provides detailed instructions for deploying the ImpactIQ Governance Workload to Microsoft Fabric.

## Deployment Options

There are three deployment scenarios:

1. **Development Mode** - Local development and testing
2. **Organizational Deployment** - Deploy to your organization's Fabric tenant
3. **Partner Publishing** - Publish to the Fabric Workload Hub for all Fabric customers

## Option 1: Development Mode

Perfect for testing and development.

### Prerequisites Checklist

- [ ] Node.js 16+ installed
- [ ] PowerShell 7 installed
- [ ] .NET SDK installed
- [ ] Azure CLI installed
- [ ] Fabric workspace with capacity assigned
- [ ] Fabric Developer Mode enabled

### Setup Steps

#### 1. Install Dependencies

```bash
cd Workload
npm install
```

#### 2. Run Setup Script

```powershell
cd ../scripts/Setup
pwsh ./Setup.ps1 -WorkloadName "Org.ImpactIQGovernance"
```

The setup script will:
- Create an Entra (Azure AD) app registration
- Configure redirect URIs
- Generate `.env.dev` configuration
- Update manifest files with your workload name

#### 3. Configure Entra App (if creating new)

If you created a new Entra app, configure these settings:

**API Permissions:**
- Microsoft Fabric: `Workspace.Read.All`
- Microsoft Fabric: `Item.Execute.All`
- Microsoft Fabric: `Dataset.Read.All`

**Authentication:**
- Platform: Single-page application (SPA)
- Redirect URIs: `http://localhost:60006`

**Token Configuration:**
- Optional claims: `email`, `preferred_username`

#### 4. Enable Developer Mode

**Admin Portal Settings:**
1. Go to [Fabric Admin Portal](https://app.fabric.microsoft.com/admin-portal/tenantSettings)
2. Navigate to **Developer Settings**
3. Enable **Users can create and use developer items**
4. Apply to your test users or entire organization

**User Settings:**
1. Click your profile icon
2. Go to **Developer Settings**
3. Toggle **Developer mode** ON
4. Select your development workspace

#### 5. Start Development Environment

**Terminal 1 - Frontend:**
```powershell
cd scripts/Run
pwsh ./StartDevServer.ps1
```

**Terminal 2 - Gateway:**
```powershell
cd scripts/Run
pwsh ./StartDevGateway.ps1
```

#### 6. Test Your Workload

1. Navigate to: `https://app.fabric.microsoft.com/workloadhub/detail/Org.ImpactIQGovernance.Product?experience=fabric-developer`
2. You should see the ImpactIQ Governance Workload
3. Click **Governance Analyzer** to create an item
4. Select your development workspace
5. The item editor should open successfully

### Troubleshooting Development Mode

**Problem**: Workload doesn't appear in Fabric
- **Check**: Developer mode enabled in admin settings AND user settings
- **Check**: Dev gateway is running and shows "Connected"
- **Check**: Browser console for errors (F12)

**Problem**: Authentication fails
- **Check**: Entra app redirect URI matches your dev URL
- **Check**: API permissions are granted and admin consented
- **Check**: `.env.dev` has correct FRONTEND_APPID

**Problem**: Item editor shows blank page
- **Check**: Dev server is running on correct port (60006)
- **Check**: No errors in browser console
- **Check**: Webpack compiled successfully (check terminal)

## Option 2: Organizational Deployment

Deploy to your organization's Fabric tenant for production use.

### Prerequisites

- [ ] Completed development and testing
- [ ] Production Entra app created
- [ ] Fabric capacity for workload hosting
- [ ] Admin access to Fabric tenant

### Deployment Steps

#### 1. Build Production Bundle

```bash
cd Workload
npm run build:prod
```

This creates optimized files in `build/Frontend/`.

#### 2. Create Environment Configuration

Create `.env.prod`:
```bash
WORKLOAD_NAME=Org.ImpactIQGovernance
WORKLOAD_VERSION=1.0.0
WORKLOAD_HOSTING_TYPE=PowerBIEmbed
FRONTEND_APPID=<your-prod-app-id>
FRONTEND_URL=https://your-workload-host.azurewebsites.net
NODE_ENV=production
```

#### 3. Package Manifest

```powershell
cd scripts/Build
pwsh ./BuildManifestPackage.ps1
```

This creates `Org.ImpactIQGovernance.{version}.nupkg`.

#### 4. Host Frontend

Deploy the `build/Frontend/` folder to your hosting solution:

**Option A: Azure App Service**
```bash
# Using Azure CLI
az webapp up --name impactiq-workload --resource-group MyResourceGroup --plan MyAppServicePlan
```

**Option B: Azure Static Web Apps**
```bash
# Deploy via GitHub Actions or Azure CLI
az staticwebapp create --name impactiq-workload --resource-group MyResourceGroup
```

**Option C: Custom Hosting**
- Deploy `build/Frontend/` to any web server
- Ensure HTTPS is enabled
- Configure CORS for Fabric domains

#### 5. Update Entra App

Update your production Entra app:
- Add production redirect URI: `https://your-workload-host.azurewebsites.net`
- Remove development URIs for security
- Ensure API permissions are admin-consented

#### 6. Register Workload in Fabric

```powershell
cd scripts/Deploy
pwsh ./DeployWorkload.ps1 -Environment "Production"
```

This uploads the workload package to your Fabric tenant.

#### 7. Enable Workload

**Admin Portal:**
1. Go to Fabric Admin Portal → Tenant Settings
2. Find your workload in the list
3. Enable it for specific users/groups or entire organization
4. Save changes

#### 8. Verify Deployment

1. Navigate to Fabric Workload Hub
2. Search for "ImpactIQ Governance"
3. Install the workload to a workspace
4. Create a Governance Analyzer item
5. Verify functionality

### Production Checklist

- [ ] Build passes without errors
- [ ] All assets load correctly (images, translations)
- [ ] Authentication works with production app
- [ ] Can create and edit items
- [ ] Lakehouse integration works
- [ ] Notebook execution succeeds
- [ ] Performance is acceptable
- [ ] Error handling works correctly

## Option 3: Partner Publishing

Publish to Fabric Workload Hub for all Fabric customers.

### Prerequisites

- [ ] Microsoft Partner Network membership
- [ ] Completed Fabric workload certification
- [ ] Commercial marketplace account
- [ ] Support infrastructure in place

### Publishing Process

#### 1. Register as Publisher

1. Join [Microsoft Partner Network](https://partner.microsoft.com/)
2. Complete partner verification
3. Set up commercial marketplace account
4. Review [publishing guidelines](https://learn.microsoft.com/fabric/extensibility-toolkit/publishing-overview)

#### 2. Update Workload Name

For public workloads, use your publisher ID:
```
WORKLOAD_NAME=PublisherID.ImpactIQGovernance
```

Update in:
- `.env.prod`
- `Manifest/WorkloadManifest.xml`
- `Manifest/Product.json`

#### 3. Prepare Marketing Materials

Required assets:
- **Workload icon**: 256x256 PNG
- **Hub banner**: 1920x1080 PNG
- **Screenshots**: At least 3 screenshots
- **Video**: Optional demo video (YouTube/Vimeo)
- **Documentation**: Complete user guide
- **Support links**: Documentation, help, terms, privacy

Update `Manifest/Product.json` with asset URLs.

#### 4. Complete Certification

**Technical Certification:**
- Security review
- Performance testing
- API compliance
- Error handling
- Data privacy

**Business Certification:**
- Support plan
- SLA commitments
- Pricing model
- Terms of service

#### 5. Submit for Review

```powershell
cd scripts/Deploy
pwsh ./SubmitToMarketplace.ps1
```

Follow the submission wizard to:
- Upload workload package
- Provide metadata
- Submit for Microsoft review

#### 6. Microsoft Review

Timeline: 2-4 weeks

Review process includes:
- Technical validation
- Security audit
- Business terms review
- Asset quality check

#### 7. Go Live

Once approved:
1. Workload appears in Fabric Workload Hub
2. All Fabric customers can discover and install
3. Monitor usage and feedback
4. Provide support via published channels

### Post-Publishing

**Monitoring:**
- Track installation metrics
- Monitor support requests
- Collect user feedback
- Review error logs

**Updates:**
- Submit updated versions via same process
- Provide release notes
- Ensure backward compatibility
- Test thoroughly before submission

## Configuration Reference

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `WORKLOAD_NAME` | Unique workload identifier | `Org.ImpactIQGovernance` |
| `WORKLOAD_VERSION` | Semantic version | `1.0.0` |
| `WORKLOAD_HOSTING_TYPE` | Hosting model | `PowerBIEmbed` |
| `FRONTEND_APPID` | Entra app client ID | `12345678-1234-...` |
| `FRONTEND_URL` | Frontend hosting URL | `https://...` |
| `NODE_ENV` | Environment | `development` / `production` |

### Manifest Structure

```
Manifest/
├── WorkloadManifest.xml       # Core workload definition
├── Product.json              # UI metadata and configuration
├── ManifestPackage.nuspec    # NuGet package definition
├── items/
│   └── [ItemType]/
│       ├── [ItemType].xml    # Item type definition
│       └── [ItemType].json   # Item UI metadata
└── assets/
    ├── images/              # Icons and screenshots
    └── locales/            # Translations
```

## Security Best Practices

1. **Never commit secrets**: Use `.env` files (gitignored)
2. **Rotate keys regularly**: Update Entra app secrets periodically
3. **Least privilege**: Request only necessary API permissions
4. **Audit logs**: Monitor workload usage and errors
5. **HTTPS only**: Never use HTTP in production
6. **Input validation**: Validate all user inputs
7. **Error handling**: Don't expose sensitive info in errors

## Performance Optimization

1. **Code splitting**: Use dynamic imports for large components
2. **Lazy loading**: Load item editors on demand
3. **Caching**: Cache API responses appropriately
4. **Compression**: Enable gzip/brotli compression
5. **CDN**: Use CDN for static assets
6. **Monitoring**: Track load times and errors

## Support and Resources

- **Documentation**: [Fabric Extensibility Toolkit](https://learn.microsoft.com/fabric/extensibility-toolkit)
- **Samples**: [Microsoft Fabric Tools Workload](https://github.com/microsoft/Microsoft-Fabric-tools-workload)
- **Community**: [Fabric Community Forums](https://community.fabric.microsoft.com/)
- **Issues**: [GitHub Issues](https://github.com/BeSmarterWithData/ImpactIQ-SemanticLinkLabs/issues)

## Next Steps

After deployment:

1. **User Documentation**: Create user guides and tutorials
2. **Training**: Provide training for your organization
3. **Feedback Loop**: Collect and act on user feedback
4. **Iterate**: Regular updates and improvements
5. **Monitor**: Track usage and performance
6. **Support**: Provide responsive support

---

**Need Help?** Open an issue on [GitHub](https://github.com/BeSmarterWithData/ImpactIQ-SemanticLinkLabs/issues) or check the [discussions](https://github.com/BeSmarterWithData/ImpactIQ-SemanticLinkLabs/discussions).
