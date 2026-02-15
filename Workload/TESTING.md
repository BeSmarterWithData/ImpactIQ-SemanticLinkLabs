# Testing and Validation Guide

This guide provides comprehensive testing procedures for the ImpactIQ Governance Fabric Workload.

## Pre-Deployment Validation

### 1. Manifest Validation

Validate all manifest files are properly structured:

```bash
# Navigate to Workload directory
cd Workload

# Validate XML files (if xmllint is available)
xmllint --noout --schema Manifest/WorkloadDefinition.xsd Manifest/WorkloadManifest.xml
xmllint --noout --schema Manifest/ItemDefinition.xsd Manifest/items/GovernanceAnalyzerItem/GovernanceAnalyzerItem.xml

# Validate JSON files
node -e "console.log(JSON.parse(require('fs').readFileSync('Manifest/Product.json', 'utf8')))"
node -e "console.log(JSON.parse(require('fs').readFileSync('Manifest/items/GovernanceAnalyzerItem/GovernanceAnalyzerItem.json', 'utf8')))"
```

**Expected Results:**
- ✅ No XML validation errors
- ✅ All JSON files are valid
- ✅ All placeholders ({{WORKLOAD_NAME}}, etc.) are replaced in dev environment

### 2. Configuration Validation

Check all configuration files:

```bash
# Check package.json
cat package.json

# Check TypeScript config
cat tsconfig.json

# Verify environment template exists
cat .env.template
```

**Expected Results:**
- ✅ All dependencies are listed
- ✅ Scripts are properly defined
- ✅ TypeScript configuration is valid

### 3. Asset Validation

Verify all required assets exist:

```bash
# Check manifest assets
ls -lh Manifest/assets/images/
ls -lh Manifest/assets/locales/en-US/

# Check item assets
ls -lh Manifest/items/GovernanceAnalyzerItem/
```

**Expected Results:**
- ✅ Workload_Icon.png exists (256x256)
- ✅ GovernanceAnalyzerItem_Icon.png exists
- ✅ Banner and slide media images exist
- ✅ translations.json exists and is valid

### 4. TypeScript Compilation

Test TypeScript compilation without running:

```bash
# Install dependencies (if not already installed)
npm install

# Run TypeScript compiler in check mode
npx tsc --noEmit

# Check for any TypeScript errors
```

**Expected Results:**
- ✅ No compilation errors
- ✅ All imports resolve correctly
- ⚠️  Warnings are acceptable, errors are not

## Development Environment Testing

### 1. Setup Testing

Test the setup script:

```bash
# Navigate to setup scripts
cd scripts/Setup

# Run setup with test parameters
pwsh ./Setup.ps1 -WorkloadName "Org.ImpactIQGovernance.Test"
```

**Test Checklist:**
- [ ] Script runs without errors
- [ ] Entra app is created or configured
- [ ] .env.dev file is generated with correct values
- [ ] All {{WORKLOAD_NAME}} placeholders are replaced
- [ ] Redirect URIs are configured correctly

### 2. Build Testing

Test the build process:

```bash
cd Workload

# Test development build
npm run build:test

# Test production build
npm run build:prod
```

**Test Checklist:**
- [ ] Build completes without errors
- [ ] Output directory `build/Frontend` is created
- [ ] All assets are copied to build directory
- [ ] Bundle size is reasonable (< 5MB)
- [ ] Source maps are generated (dev) or excluded (prod)

### 3. Dev Server Testing

Test the development server:

```bash
# Terminal 1: Start dev server
cd scripts/Run
pwsh ./StartDevServer.ps1

# In another terminal
# Terminal 2: Start dev gateway
pwsh ./StartDevGateway.ps1
```

**Test Checklist:**
- [ ] Dev server starts on port 60006
- [ ] No compilation errors
- [ ] Webpack serves files correctly
- [ ] Hot reload works for code changes
- [ ] Dev gateway connects successfully
- [ ] Gateway shows "Connected" status

### 4. Frontend Testing

Once dev server is running, test the frontend:

**Manual Browser Tests:**

1. **Navigate to Local Server**
   - URL: `http://localhost:60006`
   - Expected: App loads without errors

2. **Check Browser Console**
   - Open DevTools (F12)
   - Look for errors or warnings
   - Expected: No critical errors

3. **Test React Components**
   - Verify main app renders
   - Check that Fluent UI components display correctly
   - Expected: Clean UI with no layout issues

4. **Test Client Initialization**
   ```javascript
   // In browser console
   console.log(window);
   // Check for any global errors
   ```

### 5. Fabric Integration Testing

Test integration with Fabric (requires Fabric Developer Mode enabled):

1. **Access Workload Hub**
   - URL: `https://app.fabric.microsoft.com/workloadhub/detail/Org.ImpactIQGovernance.Test.Product?experience=fabric-developer`
   - Expected: Workload page loads

2. **Test Item Creation**
   - Click "Governance Analyzer"
   - Select your development workspace
   - Expected: Create dialog appears

3. **Test Item Editor**
   - Item editor should open
   - UI should render correctly
   - Expected: Governance Analyzer interface loads

4. **Test Functionality**
   - Click "Start Analysis" button
   - Check browser console for API calls
   - Expected: Button responds, no JavaScript errors

## Component Testing

### GovernanceAnalyzerItemEditor

Test the main item editor component:

```typescript
// Create a test file: app/items/GovernanceAnalyzerItem/__tests__/GovernanceAnalyzerItemEditor.test.tsx

import { render, screen } from '@testing-library/react';
import { GovernanceAnalyzerItemEditor } from '../GovernanceAnalyzerItemEditor';

test('renders governance analyzer title', () => {
  render(<GovernanceAnalyzerItemEditor itemId="test" workspaceId="test-ws" />);
  expect(screen.getByText(/Governance Analyzer/i)).toBeInTheDocument();
});

test('renders feature cards', () => {
  render(<GovernanceAnalyzerItemEditor itemId="test" workspaceId="test-ws" />);
  expect(screen.getByText(/Impact Analysis/i)).toBeInTheDocument();
  expect(screen.getByText(/Usage Tracking/i)).toBeInTheDocument();
  expect(screen.getByText(/Comprehensive Metadata/i)).toBeInTheDocument();
});
```

### GovernanceClient

Test the API client:

```typescript
// Create a test file: app/clients/__tests__/GovernanceClient.test.ts

import { createGovernanceClient } from '../GovernanceClient';

test('creates client instance', () => {
  const client = createGovernanceClient('ws-id', 'lakehouse');
  expect(client).toBeDefined();
});

test('trigger analysis returns result', async () => {
  const client = createGovernanceClient('ws-id', 'lakehouse');
  const result = await client.triggerAnalysis(['Sales']);
  expect(result.status).toBe('initiated');
  expect(result.config?.workspaceNames).toContain('Sales');
});
```

## Integration Testing

### Python-TypeScript Integration

Test the integration between workload and GovernanceNotebook:

1. **Create Test Notebook**
   ```python
   # In Fabric, create a test notebook
   from workload_integration import create_workload_integration
   
   integration = create_workload_integration(
       workspace_id="test-workspace-id",
       lakehouse_name="TestLakehouse"
   )
   
   # Test trigger
   result = integration.trigger_governance_analysis(
       workspace_names=["All"],
       max_parallel_workers=2
   )
   print(result)
   ```

2. **Verify Results**
   - Expected: Function returns without errors
   - Expected: Configuration is properly formatted

### End-to-End Testing

Full workflow test:

1. **Setup**
   - Create test workspace in Fabric
   - Create test lakehouse
   - Deploy workload to dev environment

2. **Create Item**
   - Navigate to workload hub
   - Create Governance Analyzer item
   - Name: "Test Governance Analyzer"

3. **Configure Item**
   - Open item editor
   - Verify lakehouse connection
   - Expected: UI shows configuration options

4. **Execute Analysis**
   - Click "Start Analysis"
   - Monitor execution
   - Expected: Analysis starts without errors

5. **View Results**
   - Click "Refresh Results"
   - Expected: Results load (even if empty)

## Performance Testing

### Build Performance

```bash
# Time the build process
time npm run build:prod

# Check bundle size
du -sh build/Frontend/
```

**Target Metrics:**
- Build time: < 2 minutes
- Bundle size: < 5 MB
- Gzipped size: < 1 MB

### Runtime Performance

Use browser DevTools:

1. **Load Time**
   - Open Network tab
   - Reload page
   - Target: < 2 seconds initial load

2. **Memory Usage**
   - Open Memory profiler
   - Take heap snapshot
   - Target: < 50 MB memory usage

3. **Render Performance**
   - Open Performance tab
   - Record interaction
   - Target: 60 FPS, no jank

## Security Testing

### 1. Dependency Audit

```bash
# Check for vulnerable dependencies
npm audit

# Fix automatically if possible
npm audit fix
```

**Expected Results:**
- ✅ No high or critical vulnerabilities
- ⚠️  Low/moderate vulnerabilities documented

### 2. Authentication Testing

- [ ] Verify Entra app permissions are minimal
- [ ] Test authentication flow works
- [ ] Verify tokens are not exposed in console
- [ ] Check that sensitive data isn't logged

### 3. API Security

- [ ] Verify HTTPS in production
- [ ] Check CORS configuration
- [ ] Test error messages don't expose sensitive info
- [ ] Verify input validation

## Deployment Testing

### Test Deployment

Before production deployment:

```bash
# Build for test environment
npm run build:test

# Package manifest
cd scripts/Build
pwsh ./BuildManifestPackage.ps1

# Verify package
ls -lh *.nupkg
```

**Test Checklist:**
- [ ] Package is created successfully
- [ ] Package size is reasonable (< 10 MB)
- [ ] All manifest files are included
- [ ] All assets are included

### Production Validation

After deployment to production:

1. **Workload Hub**
   - Verify workload appears in hub
   - Check all images load
   - Verify descriptions are correct

2. **Installation**
   - Install to test workspace
   - Create sample item
   - Verify functionality

3. **Cross-Browser Testing**
   - Test in Edge
   - Test in Chrome
   - Test in Firefox
   - Test in Safari (if available)

4. **User Acceptance Testing**
   - Have test users create items
   - Collect feedback
   - Document any issues

## Regression Testing

After any code changes:

- [ ] Re-run manifest validation
- [ ] Re-run build tests
- [ ] Re-test item creation
- [ ] Re-test item editor
- [ ] Re-test analysis functionality
- [ ] Check for console errors
- [ ] Verify no new warnings

## Common Issues and Solutions

### Issue: Build Fails with Module Not Found

**Solution:**
```bash
rm -rf node_modules package-lock.json
npm install
```

### Issue: Dev Gateway Won't Connect

**Solution:**
- Check that dev server is running first
- Verify .env.dev has correct values
- Check Fabric Developer Mode is enabled
- Restart both dev server and gateway

### Issue: Workload Doesn't Appear in Fabric

**Solution:**
- Verify Developer Mode is enabled in admin settings
- Verify Developer Mode is enabled in user settings
- Check the workload URL is correct
- Clear browser cache

### Issue: TypeScript Errors

**Solution:**
```bash
# Regenerate TypeScript config
npx tsc --init

# Check for type definition issues
npm install --save-dev @types/react @types/react-dom
```

## Testing Checklist Summary

Before considering the workload complete:

**Structure:**
- [ ] All directories created correctly
- [ ] All manifest files present and valid
- [ ] All assets included
- [ ] Scripts directory populated

**Configuration:**
- [ ] package.json is complete
- [ ] tsconfig.json is valid
- [ ] webpack.config.js works
- [ ] .env.template provided

**Code:**
- [ ] TypeScript compiles without errors
- [ ] React components render correctly
- [ ] API client is implemented
- [ ] Python integration helper created

**Documentation:**
- [ ] README.md is comprehensive
- [ ] DEPLOYMENT.md covers all scenarios
- [ ] Code comments are clear
- [ ] Examples are provided

**Functionality:**
- [ ] Dev environment runs
- [ ] Item can be created in Fabric
- [ ] Editor loads without errors
- [ ] Basic interactions work

**Quality:**
- [ ] No critical security vulnerabilities
- [ ] Performance is acceptable
- [ ] Error handling is present
- [ ] Logging is appropriate

---

**Next Steps After Testing:**

1. Document all test results
2. Address any issues found
3. Get user feedback
4. Iterate and improve
5. Prepare for production deployment

**For Questions or Issues:**

Open an issue on [GitHub](https://github.com/BeSmarterWithData/ImpactIQ-SemanticLinkLabs/issues) with:
- Test results
- Error messages
- Environment details
- Steps to reproduce
