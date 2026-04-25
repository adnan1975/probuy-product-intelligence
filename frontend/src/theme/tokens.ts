export const colors = {
  primaryNavy: '#0B1F3A',
  secondaryNavy: '#1F3A5F',
  accentTeal: '#00A8A8',
  accentTealHover: '#008E8E',
  accentTealMuted: '#E6FFFA',
  background: '#F7F9FC',
  surface: '#FFFFFF',
  textPrimary: '#1A1A1A',
  textSecondary: '#6B7280',
  textHelper: '#94A3B8',
  border: '#E2E8F0',
  success: '#16A34A',
  warning: '#F59E0B',
  danger: '#DC2626',
  rowHover: '#E6FFFA',
  tableStripe: '#F9FAFB',
} as const;

export const radii = {
  none: '0',
  sm: '4px',
  md: '10px',
  lg: '10px',
  xl: '16px',
  full: '9999px',
} as const;

export const shadows = {
  sm: '0 2px 8px rgba(11, 31, 58, 0.06)',
  md: '0 8px 24px rgba(11, 31, 58, 0.08)',
  lg: '0 14px 30px rgba(11, 31, 58, 0.1)',
} as const;

export const spacing = {
  xxs: '4px',
  xs: '8px',
  sm: '12px',
  md: '16px',
  lg: '24px',
  xl: '32px',
  xxl: '40px',
} as const;

export const typography = {
  fontFamily: "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
  sizes: {
    xs: '12px',
    sm: '14px',
    md: '16px',
    lg: '20px',
    xl: '24px',
    xxl: '32px',
  },
  weights: {
    regular: 400,
    medium: 500,
    semibold: 600,
    bold: 700,
  },
  lineHeights: {
    tight: 1.2,
    normal: 1.5,
    relaxed: 1.65,
  },
  hierarchy: {
    pageTitle: {
      size: '32px',
      weight: 700,
      lineHeight: 1.2,
    },
    sectionTitle: {
      size: '20px',
      weight: 600,
      lineHeight: 1.3,
    },
    body: {
      size: '16px',
      weight: 400,
      lineHeight: 1.5,
    },
    helper: {
      size: '12px',
      weight: 400,
      lineHeight: 1.4,
    },
  },
} as const;

export const themeTokens = {
  colors,
  radii,
  shadows,
  spacing,
  typography,
} as const;

export type ThemeTokens = typeof themeTokens;
