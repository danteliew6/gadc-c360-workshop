/**
 * McDonald's Philippines (GADC) white-label palette — demo. Brand chrome uses
 * McDonald's red + golden yellow on a near-black / off-white ground. Semantic
 * KPI tones are kept distinct from the brand red so a "high churn" red never
 * reads as decoration.
 */
export const COLORS = {
  brand: '#DA291C', // McDonald's red — chrome / primary
  gold: '#FFC72C', // golden yellow — loyalty / accent
  ink: '#292929', // near-black text
  good: '#1E8E3E', // green — healthy / low risk
  warn: '#C77700', // amber — medium risk / attention
  bad: '#C62828', // red — high risk / churn
  info: '#1F6FEB', // blue — neutral metric
  muted: '#6B7280',
  text: '#292929',
} as const;

/** Categorical chart series colors (gold-forward, brand-consistent). */
export const SEQUENCE = ['#FFC72C', '#DA291C', '#1F6FEB', '#1E8E3E', '#C77700', '#6B7280'];

/** Churn-risk band → semantic tone. */
export const CHURN_TONE: Record<string, keyof typeof COLORS> = {
  LOW: 'good',
  MEDIUM: 'warn',
  HIGH: 'bad',
};

/** MyMcDonald's Rewards status tiers, low → high. */
export const TIER_ORDER = ['Member', 'Silver', 'Gold', 'Platinum'];

/** Status-tier → chip tone (Platinum/Gold lean gold; Member neutral). */
export const TIER_TONE: Record<string, keyof typeof COLORS> = {
  Member: 'muted',
  Silver: 'info',
  Gold: 'gold',
  Platinum: 'brand',
};

/** Tier-status (held vs qualified) → semantic tone. */
export const TIER_STATUS_TONE: Record<string, keyof typeof COLORS> = {
  'On track': 'good',
  'Upgrade eligible': 'gold',
  'Downgrade risk': 'bad',
};
