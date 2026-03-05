import React, { useState, useEffect } from 'react';
import {
  Button,
  Text,
  Card,
  CardHeader,
  Divider,
  makeStyles,
  tokens,
  Spinner,
} from '@fluentui/react-components';
import {
  DatabaseRegular,
  DocumentTextRegular,
  ChartMultipleRegular,
} from '@fluentui/react-icons';
import { createGovernanceClient, GovernanceSummary } from '../../clients/GovernanceClient';

const useStyles = makeStyles({
  container: {
    display: 'flex',
    flexDirection: 'column',
    padding: tokens.spacingVerticalXL,
    gap: tokens.spacingVerticalL,
    maxWidth: '1200px',
    margin: '0 auto',
  },
  header: {
    fontSize: tokens.fontSizeHero800,
    fontWeight: tokens.fontWeightSemibold,
    color: tokens.colorBrandForeground1,
    marginBottom: tokens.spacingVerticalM,
  },
  description: {
    fontSize: tokens.fontSizeBase300,
    color: tokens.colorNeutralForeground2,
    marginBottom: tokens.spacingVerticalXL,
  },
  featureGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))',
    gap: tokens.spacingVerticalL,
  },
  featureCard: {
    padding: tokens.spacingVerticalXL,
  },
  featureIcon: {
    fontSize: '48px',
    color: tokens.colorBrandForeground1,
    marginBottom: tokens.spacingVerticalM,
  },
  featureTitle: {
    fontSize: tokens.fontSizeBase500,
    fontWeight: tokens.fontWeightSemibold,
    marginBottom: tokens.spacingVerticalS,
  },
  featureDescription: {
    fontSize: tokens.fontSizeBase300,
    color: tokens.colorNeutralForeground2,
  },
  actionSection: {
    marginTop: tokens.spacingVerticalXXL,
    padding: tokens.spacingVerticalXL,
    backgroundColor: tokens.colorNeutralBackground1,
    borderRadius: tokens.borderRadiusMedium,
  },
  buttonGroup: {
    display: 'flex',
    gap: tokens.spacingHorizontalM,
    marginTop: tokens.spacingVerticalL,
  },
});

interface GovernanceAnalyzerItemEditorProps {
  itemId?: string;
  workspaceId?: string;
}

export const GovernanceAnalyzerItemEditor: React.FC<GovernanceAnalyzerItemEditorProps> = ({
  itemId,
  workspaceId,
}) => {
  const styles = useStyles();
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [results, setResults] = useState<GovernanceSummary | null>(null);
  const [error, setError] = useState<string | null>(null);

  const client = workspaceId 
    ? createGovernanceClient(workspaceId, 'PowerBIGovernance')
    : null;

  useEffect(() => {
    // Load existing results on mount
    if (client) {
      loadResults();
    }
  }, [workspaceId]);

  const loadResults = async () => {
    if (!client) return;
    
    setIsLoading(true);
    try {
      const data = await client.getResults();
      setResults(data.summary);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load results');
    } finally {
      setIsLoading(false);
    }
  };

  const handleStartAnalysis = async () => {
    if (!client) {
      alert('Workspace ID is required to start analysis');
      return;
    }

    setIsAnalyzing(true);
    setError(null);
    
    try {
      const result = await client.triggerAnalysis();
      alert(`Analysis started successfully! Run ID: ${result.runId}`);
      
      // Optionally reload results after a delay
      setTimeout(() => {
        loadResults();
        setIsAnalyzing(false);
      }, 2000);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to start analysis');
      setIsAnalyzing(false);
    }
  };

  return (
    <div className={styles.container}>
      <div>
        <Text className={styles.header}>Governance Analyzer</Text>
        <Text className={styles.description}>
          Analyze your Power BI and Fabric environment for impact, usage, and governance insights.
          Identify downstream impacts of model changes, discover unused objects, and maintain
          complete visibility across all workspaces.
        </Text>
      </div>

      <Divider />

      <div className={styles.featureGrid}>
        <Card className={styles.featureCard}>
          <CardHeader
            image={<ChartMultipleRegular className={styles.featureIcon} />}
            header={<Text className={styles.featureTitle}>Impact Analysis</Text>}
            description={
              <Text className={styles.featureDescription}>
                Understand the downstream impact of data model changes. See which visuals and
                dashboards will be affected before making changes.
              </Text>
            }
          />
        </Card>

        <Card className={styles.featureCard}>
          <CardHeader
            image={<DatabaseRegular className={styles.featureIcon} />}
            header={<Text className={styles.featureTitle}>Usage Tracking</Text>}
            description={
              <Text className={styles.featureDescription}>
                Identify which tables, columns, and measures are actively used. Discover unused
                objects that can be safely removed to optimize performance.
              </Text>
            }
          />
        </Card>

        <Card className={styles.featureCard}>
          <CardHeader
            image={<DocumentTextRegular className={styles.featureIcon} />}
            header={<Text className={styles.featureTitle}>Comprehensive Metadata</Text>}
            description={
              <Text className={styles.featureDescription}>
                Extract complete metadata from reports, models, and dataflows. Store everything in
                a Fabric Lakehouse for easy analysis and reporting.
              </Text>
            }
          />
        </Card>
      </div>

      <div className={styles.actionSection}>
        <Text className={styles.featureTitle}>Getting Started</Text>
        <Text className={styles.featureDescription}>
          To begin analyzing your environment, you'll need:
          <ul>
            <li>A Fabric Lakehouse connected to this workspace</li>
            <li>Access to the workspaces you want to analyze</li>
            <li>The GovernanceNotebook configured and ready to run</li>
          </ul>
        </Text>

        {error && (
          <Text style={{ color: 'red', marginTop: '10px' }}>
            Error: {error}
          </Text>
        )}

        {isLoading && (
          <div style={{ marginTop: '10px' }}>
            <Spinner label="Loading results..." />
          </div>
        )}

        {results && (
          <div style={{ marginTop: '20px', padding: '15px', backgroundColor: '#f5f5f5', borderRadius: '8px' }}>
            <Text className={styles.featureTitle}>Latest Results</Text>
            <ul>
              <li>Total Reports: {results.totalReports}</li>
              <li>Total Models: {results.totalModels}</li>
              <li>Total Dataflows: {results.totalDataflows}</li>
              <li>Unused Objects: {results.unusedObjects}</li>
              <li>Broken Visuals: {results.brokenVisuals}</li>
            </ul>
          </div>
        )}

        <div className={styles.buttonGroup}>
          <Button
            appearance="primary"
            onClick={handleStartAnalysis}
            disabled={isAnalyzing || !client}
          >
            {isAnalyzing ? 'Analyzing...' : 'Start Analysis'}
          </Button>
          <Button
            appearance="secondary"
            onClick={loadResults}
            disabled={isLoading || !client}
          >
            Refresh Results
          </Button>
          <Button
            appearance="secondary"
            onClick={() => window.open('https://github.com/BeSmarterWithData/ImpactIQ-SemanticLinkLabs', '_blank')}
          >
            View Documentation
          </Button>
        </div>
      </div>
    </div>
  );
};

export default GovernanceAnalyzerItemEditor;
