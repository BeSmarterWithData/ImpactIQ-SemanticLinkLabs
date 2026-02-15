import React, { useState } from 'react';
import {
  Button,
  Text,
  Card,
  CardHeader,
  Divider,
  makeStyles,
  tokens,
} from '@fluentui/react-components';
import {
  DatabaseRegular,
  DocumentTextRegular,
  ChartMultipleRegular,
} from '@fluentui/react-icons';

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

  const handleStartAnalysis = () => {
    setIsAnalyzing(true);
    // TODO: Integrate with GovernanceNotebook.py functionality
    setTimeout(() => {
      setIsAnalyzing(false);
      alert('Analysis would start here. Integration with GovernanceNotebook.py pending.');
    }, 1000);
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
        <div className={styles.buttonGroup}>
          <Button
            appearance="primary"
            onClick={handleStartAnalysis}
            disabled={isAnalyzing}
          >
            {isAnalyzing ? 'Analyzing...' : 'Start Analysis'}
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
