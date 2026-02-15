# Quick Start Guide: Making Your Workload Available to Other Organizations

This guide walks you through the specific steps to make the ImpactIQ Governance Workload available to other organizations.

## 📋 Overview: Your Three Distribution Options

### 1. 🏢 **Organizational Deployment** (Recommended to start)
**Best for:** Making the workload available within your own organization or to specific partner organizations  
**Timeline:** 1-2 weeks  
**Complexity:** Medium  
**Reach:** Your Fabric tenant only

### 2. 🌍 **Public Partner Publishing** (For wider distribution)
**Best for:** Making the workload available to all Fabric customers worldwide  
**Timeline:** 2-3 months (includes certification)  
**Complexity:** High  
**Reach:** Global (Fabric Workload Hub)

### 3. 🎁 **Open Source Distribution** (For maximum flexibility)
**Best for:** Allowing organizations to self-deploy from your GitHub repo  
**Timeline:** Immediate  
**Complexity:** Low  
**Reach:** Anyone with access to the repo

---

## 🚀 Recommended Path: Start with Option 1, Scale to Options 2 or 3

---

## Option 1: Organizational Deployment (Your Tenant)

### Step 1: Complete Development Setup
```bash
# Navigate to the Workload directory
cd Workload

# Install dependencies
npm install

# Run setup to configure your environment
cd ../scripts/Setup
pwsh ./Setup.ps1 -WorkloadName "Org.ImpactIQGovernance"
```

**What this does:**
- Creates/configures Entra (Azure AD) app
- Generates environment configurations
- Prepares manifest files

### Step 2: Build Production Version
```bash
# Build optimized frontend
cd ../../Workload
npm run build:prod

# Package the manifest
cd ../scripts/Build
pwsh ./BuildManifestPackage.ps1
```

**Output:** You'll get:
- `build/Frontend/` folder with production app
- `Org.ImpactIQGovernance.1.0.0.nupkg` manifest package

### Step 3: Host the Frontend
Choose one option:

**A. Azure App Service (Recommended)**
```bash
# Create and deploy to Azure App Service
az webapp up --name impactiq-workload --resource-group MyResourceGroup --plan MyAppServicePlan
```

**B. Azure Static Web Apps**
```bash
az staticwebapp create --name impactiq-workload --resource-group MyResourceGroup
```

**C. GitHub Pages (For testing)**
- Enable GitHub Pages on your repo
- Deploy the `build/Frontend/` contents

### Step 4: Configure Entra App for Production
1. Go to [Azure Portal](https://portal.azure.com) → Entra ID → App Registrations
2. Find your app (created by setup script)
3. Update **Authentication** → Add redirect URI: `https://your-app-url.azurewebsites.net`
4. Ensure **API Permissions** are admin-consented:
   - Microsoft Fabric: `Workspace.Read.All`
   - Microsoft Fabric: `Item.Execute.All`
   - Microsoft Fabric: `Dataset.Read.All`

### Step 5: Deploy to Your Fabric Tenant
```bash
# Deploy the workload package
cd ../scripts/Deploy
pwsh ./DeployWorkload.ps1 -Environment "Production"
```

**What this does:** Uploads the NuGet package to your Fabric tenant's workload registry

### Step 6: Enable in Fabric Admin Portal
1. Go to [Fabric Admin Portal](https://app.fabric.microsoft.com/admin-portal/tenantSettings)
2. Navigate to **Tenant Settings** → **Developer Settings**
3. Find "ImpactIQ Governance" in the workload list
4. Enable for your organization (or specific security groups)
5. Save changes

### Step 7: Verify Deployment
1. Navigate to Fabric Workload Hub in your tenant
2. Search for "ImpactIQ Governance"
3. Install to a test workspace
4. Create a "Governance Analyzer" item
5. Test functionality

**✅ Success!** Your organization can now use the workload.

---

## Option 2: Partner Publishing (Global Distribution)

### Prerequisites
- [ ] Microsoft Partner Network membership
- [ ] Microsoft commercial marketplace account
- [ ] Support infrastructure (documentation, help desk)
- [ ] Completed testing and security review

### Step 1: Join Microsoft Partner Network
1. Go to [Microsoft Partner Network](https://partner.microsoft.com/)
2. Create/login to your account
3. Complete partner verification process
4. Set up your partner profile

### Step 2: Set Up Commercial Marketplace Account
1. Navigate to [Partner Center](https://partner.microsoft.com/dashboard/commercial-marketplace)
2. Complete marketplace registration
3. Set up payout and tax profiles
4. Agree to publisher terms

### Step 3: Update Workload for Publishing
Change your workload name to use your Publisher ID:

**Update `.env.prod`:**
```bash
WORKLOAD_NAME=YourPublisherID.ImpactIQGovernance
WORKLOAD_VERSION=1.0.0
```

**Update all manifest files:**
- `Workload/Manifest/WorkloadManifest.xml`
- `Workload/Manifest/Product.json`
- `Workload/Manifest/items/GovernanceAnalyzerItem/GovernanceAnalyzerItem.xml`

### Step 4: Prepare Marketing Materials
Required assets (update in `Workload/Manifest/Product.json`):

- [ ] **Workload Icon**: High-quality 256x256 PNG
- [ ] **Hub Banner**: Professional 1920x1080 PNG
- [ ] **Screenshots**: At least 3-5 high-quality screenshots
- [ ] **Demo Video**: YouTube or Vimeo link (optional but recommended)
- [ ] **Documentation**: Public URL to your docs
- [ ] **Support Links**: Help, privacy policy, terms of service

### Step 5: Complete Fabric Workload Certification
Work with Microsoft to complete:

**Technical Requirements:**
- Security review (vulnerability scanning)
- Performance testing (load testing)
- API compliance verification
- Error handling validation
- Data privacy audit

**Business Requirements:**
- Support plan documentation
- SLA commitments
- Pricing model (if applicable)
- Terms of service
- Privacy policy

### Step 6: Submit to Microsoft
```powershell
cd scripts/Deploy
pwsh ./SubmitToMarketplace.ps1
```

Follow the submission wizard to:
1. Upload your workload package
2. Provide metadata and descriptions
3. Upload marketing materials
4. Submit for Microsoft review

### Step 7: Wait for Approval
- **Timeline:** 2-4 weeks
- **Process:** Microsoft reviews technical, security, and business aspects
- **Updates:** You'll receive feedback via Partner Center

### Step 8: Go Live
Once approved:
1. Workload appears in global Fabric Workload Hub
2. All Fabric customers can discover and install
3. Monitor adoption via Partner Center analytics

**✅ Success!** Your workload is now available globally.

---

## Option 3: Open Source Distribution (GitHub)

### Step 1: Prepare Repository Documentation
Already done! Your repo has:
- ✅ Complete Workload structure in `/Workload` directory
- ✅ Comprehensive documentation (README, DEPLOYMENT, TESTING, etc.)
- ✅ Setup automation scripts

### Step 2: Add Installation Instructions to Main README
Add a prominent section to the main README:

```markdown
## 🎁 Deploy to Your Organization

Want to deploy ImpactIQ Governance as a Fabric Workload in your tenant?

1. Clone this repository
2. Follow the [Workload Deployment Guide](./Workload/DEPLOYMENT.md)
3. Run the setup scripts to configure for your tenant
4. Deploy using the automated scripts

See [Workload/README.md](./Workload/README.md) for complete instructions.
```

### Step 3: Create a Release
```bash
# Tag the current version
git tag -a v1.0.0 -m "First stable release with Fabric Workload"
git push origin v1.0.0

# Create a GitHub release
gh release create v1.0.0 \
  --title "ImpactIQ Governance v1.0.0 - Fabric Workload Edition" \
  --notes "First release including Microsoft Fabric Workload packaging"
```

### Step 4: Document Prerequisites
Create a clear prerequisites section for organizations:

**What they need:**
- Fabric tenant with capacity
- Fabric admin access
- Entra ID app registration permissions
- Azure hosting (App Service or Static Web Apps)
- Node.js, PowerShell, .NET SDK for building

### Step 5: Provide Support Channels
- GitHub Issues for bug reports
- GitHub Discussions for questions
- Optional: Discord/Slack community

**✅ Success!** Organizations can self-deploy from your repo.

---

## 📊 Comparison: Which Option to Choose?

| Aspect | Organizational | Partner Publishing | Open Source |
|--------|---------------|-------------------|-------------|
| **Setup Time** | 1-2 weeks | 2-3 months | Immediate |
| **Cost** | Azure hosting only | Partner fees + hosting | Free (GitHub) |
| **Reach** | Your tenant | Global | Anyone |
| **Control** | Full | Shared with Microsoft | Full |
| **Updates** | You control | Certification required | You control |
| **Support** | Internal only | Must provide SLA | Community-driven |
| **Discovery** | Internal only | Workload Hub | GitHub/marketing |

---

## 🎯 Recommended Approach

### Phase 1: Validate (Weeks 1-2)
1. Deploy organizationally to your tenant
2. Get internal users testing
3. Gather feedback and iterate
4. Fix any bugs or issues

### Phase 2: Scale (Months 1-2)
Choose your path:

**Path A: Keep it organizational**
- Help partner organizations deploy (sharing scripts/docs)
- Maintain control and customization
- Lower overhead

**Path B: Go public via Partner Publishing**
- Higher reach and visibility
- Microsoft marketing support
- Requires ongoing certification

**Path C: Open source for self-deployment**
- Maximum flexibility
- Community contributions
- Minimal ongoing commitment

---

## 🔧 Next Actions for You

### Immediate (This Week)
- [ ] Complete development testing
- [ ] Fix any remaining bugs
- [ ] Update documentation with your specific details
- [ ] Create Entra app for production

### Short-term (Next 2 Weeks)
- [ ] Build production version
- [ ] Set up Azure hosting
- [ ] Deploy to your Fabric tenant
- [ ] Enable for test users
- [ ] Validate end-to-end workflow

### Medium-term (Next Month)
- [ ] Decide on distribution strategy (Org/Partner/Open Source)
- [ ] Begin partner registration (if going public)
- [ ] Create marketing materials
- [ ] Set up support infrastructure

### Long-term (Next Quarter)
- [ ] Complete certification (if partner publishing)
- [ ] Launch to wider audience
- [ ] Monitor usage and feedback
- [ ] Plan v2.0 features

---

## 📞 Getting Help

If you need assistance:

1. **Fabric Workload Documentation**: [Microsoft Learn](https://learn.microsoft.com/fabric/extensibility-toolkit)
2. **Partner Center Support**: [Partner Support](https://partner.microsoft.com/support)
3. **Technical Questions**: Open issue in this repo
4. **Community**: [Fabric Community Forums](https://community.fabric.microsoft.com/)

---

## 📝 Quick Reference: Key Files to Customize

Before deployment, customize these files with your details:

1. **`Workload/Manifest/Product.json`**
   - Publisher name
   - Support links
   - Documentation URLs

2. **`Workload/Manifest/WorkloadManifest.xml`**
   - Entra app ID
   - Frontend hosting URL

3. **`Workload/.env.prod`**
   - Production settings
   - Workload name
   - Frontend URL

4. **`Workload/Manifest/assets/locales/en-US/translations.json`**
   - Display names
   - Descriptions

5. **Marketing Assets** (if partner publishing)
   - `Workload/Manifest/assets/images/Workload_Icon.png`
   - `Workload/Manifest/assets/images/Workload_Hub_Banner.png`
   - Screenshots and demo videos

---

**Good luck with your deployment! 🚀**
