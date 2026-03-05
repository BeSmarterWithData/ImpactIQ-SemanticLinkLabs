import React from 'react';
import { FluentProvider, webLightTheme } from '@fluentui/react-components';

export const theme = webLightTheme;

export const ThemeProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  return <FluentProvider theme={theme}>{children}</FluentProvider>;
};
