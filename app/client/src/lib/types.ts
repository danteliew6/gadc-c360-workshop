/** Shapes returned by the /api/* routes (see server/server.ts). */

export interface Meta {
  backend: string;
  database: string;
  members: number | null;
  stores: number | null;
  refreshed_at: string | null;
}

export interface MemberSearchRow {
  customer_id: string;
  full_name: string;
  email: string | null;
  mobile: string | null;
  region: string | null;
  city: string | null;
  current_tier: string | null;
  qualified_tier: string | null;
  tier_status: string | null;
  points_balance: number;
  rfm_segment: string | null;
  churn_risk: string | null;
  total_orders: number;
  total_spend: number;
  avg_order_value: number;
  recency_days: number;
  last_order_date: string | null;
}

export interface Facets {
  regions: string[];
  segments: string[];
  tiers: string[];
}

export interface Member {
  customer_id: string;
  full_name: string;
  first_name: string | null;
  last_name: string | null;
  email: string | null;
  mobile: string | null;
  gender: string | null;
  birth_date: string | null;
  age: number | null;
  region: string | null;
  province: string | null;
  city: string | null;
  signup_date: string | null;
  signup_channel: string | null;
  preferred_language: string | null;
  marketing_consent: boolean | null;
  // MyMcDonald's Rewards — points economy & status tiers.
  current_tier: string | null;
  current_tier_rank: number | null;
  qualified_tier: string | null;
  qualified_tier_rank: number | null;
  tier_status: string | null;
  points_balance: number;
  points_liability_php: number;
  lifetime_points_earned: number;
  lifetime_points_redeemed: number;
  points_expired: number;
  bonus_points: number;
  points_earned_12mo: number;
  redemptions_count: number;
  redemption_rate: number;
  points_to_next_tier: number;
  days_since_last_redeem: number | null;
  last_redeem_date: string | null;
  last_earn_date: string | null;
  total_orders: number;
  total_spend: number;
  avg_order_value: number;
  first_order_date: string | null;
  last_order_date: string | null;
  distinct_stores: number;
  delivery_orders: number;
  promo_orders: number;
  recency_days: number;
  tenure_days: number;
  delivery_share: number;
  r_score: number | null;
  f_score: number | null;
  m_score: number | null;
  rfm_score: string | null;
  rfm_segment: string | null;
  churn_risk: string | null;
  clv_estimate: number;
  engagement_score: number;
  favorite_store: string | null;
  favorite_category: string | null;
  preferred_channel: string | null;
  total_app_events: number;
  rewards_redeemed: number;
  active_days: number;
  last_active_date: string | null;
  refreshed_at: string | null;
}

export interface ActionEntry {
  kind: 'voucher' | 'ticket' | 'tier_change' | 'note' | 'points_adjustment' | 'reward_grant';
  created_at: string;
  summary: string;
}

export interface MemberDetail {
  member: Member;
  actions: ActionEntry[];
}

export interface RewardPerf {
  reward_id: string;
  reward_name: string;
  category: string | null;
  point_cost: number;
  est_value_php: number;
  reward_tier: string | null;
  value_per_point: number;
  is_active: boolean | null;
  redemptions: number;
  points_spent: number;
  unique_members: number;
  last_redeemed_date: string | null;
  value_delivered_php: number;
  pct_of_redemptions: number;
}

export interface RewardsResponse {
  kpis: {
    rewards: number;
    redemptions: number;
    points_spent: number;
    value_delivered_php: number;
  } | null;
  rows: RewardPerf[];
}

export interface StoreRow {
  store_id: string;
  store_name: string;
  region: string | null;
  province: string | null;
  city: string | null;
  store_format: string | null;
  has_drive_thru: boolean | null;
  has_mccafe: boolean | null;
  orders: number;
  revenue: number;
  avg_order_value: number;
  unique_customers: number;
  drive_thru_share: number;
  revenue_per_customer: number;
}

export interface StoresResponse {
  kpis: {
    stores: number;
    revenue: number;
    orders: number;
    avg_drive_thru_share: number;
    drive_thru_stores: number;
    mccafe_stores: number;
  } | null;
  top: { store_name: string; region: string | null; revenue: number }[];
  rows: StoreRow[];
}
