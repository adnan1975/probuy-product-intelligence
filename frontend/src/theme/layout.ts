import { spacing } from './tokens';

/**
 * Layout tokens intended for pricing/search screens.
 *
 * - Search bar spans full body/content width.
 * - Facets rail sits on the left of results.
 */
export const pricingLayout = {
  maxBodyWidth: '100%',
  searchBar: {
    width: '100%',
    minHeight: '44px',
    gridColumn: '1 / -1',
  },
  grid: {
    columns: '280px minmax(0, 1fr)',
    templateAreas: `
      "search search"
      "facets results"
    `,
    gap: spacing.lg,
  },
} as const;
