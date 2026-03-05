import React from 'react';
import { BrowserRouter as Router, Route, Switch } from 'react-router-dom';
import { ThemeProvider } from './theme';
import './i18n';

const App: React.FC = () => {
  return (
    <ThemeProvider>
      <Router>
        <div className="app-container">
          <Switch>
            <Route path="/" exact>
              <div style={{ padding: '20px', textAlign: 'center' }}>
                <h1>ImpactIQ Governance Workload</h1>
                <p>Power BI and Fabric Governance Solution</p>
              </div>
            </Route>
          </Switch>
        </div>
      </Router>
    </ThemeProvider>
  );
};

export default App;
