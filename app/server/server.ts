/**
 * McDonald's Philippines — Customer 360 console (AppKit server).
 *
 * READS: fast lookups over the Lakebase `serving.customer_360` and
 * `serving.store_performance` synced tables. WRITES: operational write-back into
 * the app-owned `app.*` tables (vouchers, tickets, tier changes, notes). All
 * queries are parameterized; identifiers are guarded in ./lakebase.ts.
 */
import { createApp, server } from '@databricks/appkit';
import express, { type Request, type Response } from 'express';
import { query, servingTable, appTable, initAppSchema } from './lakebase.js';

const C360 = servingTable('customer_360');
const STORES = servingTable('store_performance');
const REWARDS = servingTable('reward_performance');

/** Wrap an async route so rejections become a 500 with a JSON message. */
function route(handler: (req: Request, res: Response) => Promise<void>) {
  return (req: Request, res: Response) => {
    handler(req, res).catch((err: unknown) => {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`[api] ${req.method} ${req.path} failed:`, message);
      if (!res.headersSent) res.status(500).json({ error: message });
    });
  };
}

function clampInt(value: unknown, def: number, min: number, max: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return def;
  return Math.min(max, Math.max(min, Math.trunc(n)));
}

function str(value: unknown): string {
  if (typeof value === 'string') return value;
  if (value == null) return '';
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  return ''; // arrays / objects (e.g. repeated query params) are ignored
}

function optNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

/** The signed-in agent, from the Databricks Apps proxy headers. */
function actor(req: Request): string {
  return req.header('x-forwarded-email') ?? req.header('x-forwarded-user') ?? 'agent';
}

// Full display projection of a customer_360 row (numerics cast to float so they
// arrive as JS numbers, dates as ISO text).
const MEMBER_COLUMNS = `
  customer_id, full_name, first_name, last_name, email, mobile, gender,
  birth_date::text AS birth_date, age::int AS age, region, province, city,
  signup_date::text AS signup_date, signup_channel, preferred_language,
  marketing_consent,
  current_tier, current_tier_rank::int AS current_tier_rank,
  qualified_tier, qualified_tier_rank::int AS qualified_tier_rank, tier_status,
  points_balance::int AS points_balance, points_liability_php::float8 AS points_liability_php,
  lifetime_points_earned::int AS lifetime_points_earned,
  lifetime_points_redeemed::int AS lifetime_points_redeemed,
  points_expired::int AS points_expired, bonus_points::int AS bonus_points,
  points_earned_12mo::int AS points_earned_12mo,
  redemptions_count::int AS redemptions_count, redemption_rate::float8 AS redemption_rate,
  points_to_next_tier::int AS points_to_next_tier,
  days_since_last_redeem::int AS days_since_last_redeem,
  last_redeem_date::text AS last_redeem_date, last_earn_date::text AS last_earn_date,
  total_orders::int AS total_orders, total_spend::float8 AS total_spend,
  avg_order_value::float8 AS avg_order_value, first_order_date::text AS first_order_date,
  last_order_date::text AS last_order_date, distinct_stores::int AS distinct_stores,
  delivery_orders::int AS delivery_orders, promo_orders::int AS promo_orders,
  recency_days::int AS recency_days, tenure_days::int AS tenure_days,
  delivery_share::float8 AS delivery_share, r_score::int AS r_score, f_score::int AS f_score,
  m_score::int AS m_score, rfm_score, rfm_segment, churn_risk,
  clv_estimate::float8 AS clv_estimate, engagement_score::float8 AS engagement_score,
  favorite_store, favorite_category, preferred_channel,
  total_app_events::int AS total_app_events, rewards_redeemed::int AS rewards_redeemed,
  active_days::int AS active_days, last_active_date::text AS last_active_date,
  _refreshed_at::text AS refreshed_at`;

await createApp({
  plugins: [server()],
  onPluginsReady(appkit) {
    // Create the app-owned write-back schema/tables once at startup. Non-fatal:
    // if Lakebase isn't reachable yet, log and let per-request errors surface.
    initAppSchema().catch((err: unknown) => {
      console.error('[startup] initAppSchema failed:', err instanceof Error ? err.message : err);
    });

    appkit.server.extend((app) => {
      app.use(express.json());

      // ── Meta / freshness ────────────────────────────────────────────────
      app.get(
        '/api/meta',
        route(async (_req, res) => {
          const rows = await query<{ members: number; stores: number; refreshed_at: string | null }>(
            `SELECT
               (SELECT count(*) FROM ${C360})::int AS members,
               (SELECT count(*) FROM ${STORES})::int AS stores,
               (SELECT max(_refreshed_at) FROM ${C360})::text AS refreshed_at`
          );
          res.json({
            backend: 'lakebase',
            database: process.env.PGDATABASE ?? 'databricks_postgres',
            members: rows[0]?.members ?? null,
            stores: rows[0]?.stores ?? null,
            refreshed_at: rows[0]?.refreshed_at ?? null,
          });
        })
      );

      // ── Member search ───────────────────────────────────────────────────
      app.get(
        '/api/members',
        route(async (req, res) => {
          const q = str(req.query.q).trim();
          const region = str(req.query.region).trim();
          const segment = str(req.query.segment).trim();
          const tier = str(req.query.tier).trim();
          const limit = clampInt(req.query.limit, 50, 1, 200);
          const rows = await query(
            `SELECT customer_id, full_name, email, mobile, region, city,
                    current_tier, qualified_tier, tier_status,
                    points_balance::int AS points_balance,
                    rfm_segment, churn_risk,
                    total_orders::int AS total_orders, total_spend::float8 AS total_spend,
                    avg_order_value::float8 AS avg_order_value, recency_days::int AS recency_days,
                    last_order_date::text AS last_order_date
             FROM ${C360}
             WHERE ($1 = '' OR full_name ILIKE '%' || $1 || '%' OR customer_id ILIKE '%' || $1 || '%'
                    OR email ILIKE '%' || $1 || '%')
               AND ($2 = '' OR region = $2)
               AND ($3 = '' OR rfm_segment = $3)
               AND ($4 = '' OR current_tier = $4)
             ORDER BY total_spend DESC NULLS LAST
             LIMIT $5`,
            [q, region, segment, tier, limit]
          );
          res.json(rows);
        })
      );

      // Distinct filter values for the search controls.
      app.get(
        '/api/members/facets',
        route(async (_req, res) => {
          const [regions, segments, tiers] = await Promise.all([
            query<{ region: string }>(
              `SELECT DISTINCT region FROM ${C360} WHERE region IS NOT NULL ORDER BY region`
            ),
            query<{ rfm_segment: string }>(
              `SELECT DISTINCT rfm_segment FROM ${C360} WHERE rfm_segment IS NOT NULL ORDER BY rfm_segment`
            ),
            query<{ current_tier: string }>(
              `SELECT current_tier FROM ${C360} WHERE current_tier IS NOT NULL
               GROUP BY current_tier ORDER BY max(current_tier_rank)`
            ),
          ]);
          res.json({
            regions: regions.map((r) => r.region),
            segments: segments.map((r) => r.rfm_segment),
            tiers: tiers.map((r) => r.current_tier),
          });
        })
      );

      // ── Member 360 (profile + recent actions) ─────────────────────────────
      app.get(
        '/api/members/:id',
        route(async (req, res) => {
          const id = str(req.params.id);
          const [members, actions] = await Promise.all([
            query(`SELECT ${MEMBER_COLUMNS} FROM ${C360} WHERE customer_id = $1 LIMIT 1`, [id]),
            query(
              `SELECT kind, created_at::text AS created_at, summary FROM (
                 SELECT 'voucher' AS kind, created_at,
                        'Voucher ' || code || coalesce(' · ₱' || round(amount, 2)::text, '') AS summary
                 FROM ${appTable('vouchers')} WHERE customer_id = $1
                 UNION ALL
                 SELECT 'ticket', created_at,
                        coalesce(subject, '(no subject)') || ' [' || coalesce(status, 'OPEN') || ']'
                 FROM ${appTable('service_tickets')} WHERE customer_id = $1
                 UNION ALL
                 SELECT 'tier_change', created_at,
                        coalesce(old_tier, '?') || ' → ' || coalesce(new_tier, '?')
                 FROM ${appTable('tier_changes')} WHERE customer_id = $1
                 UNION ALL
                 SELECT 'note', created_at, left(body, 120)
                 FROM ${appTable('notes')} WHERE customer_id = $1
                 UNION ALL
                 SELECT 'points_adjustment', created_at,
                        (CASE WHEN points >= 0 THEN '+' ELSE '' END) || points || ' pts'
                        || coalesce(' · ' || reason, '')
                 FROM ${appTable('points_adjustments')} WHERE customer_id = $1
                 UNION ALL
                 SELECT 'reward_grant', created_at,
                        coalesce(reward_name, 'Reward')
                        || coalesce(' (' || point_cost || ' pts)', '')
                 FROM ${appTable('reward_grants')} WHERE customer_id = $1
               ) feed ORDER BY created_at DESC LIMIT 50`,
              [id]
            ),
          ]);
          if (!members.length) {
            res.status(404).json({ error: `No member with customer_id ${JSON.stringify(id)}` });
            return;
          }
          res.json({ member: members[0], actions });
        })
      );

      // ── Actions (write-back into app.*) ───────────────────────────────────
      app.post(
        '/api/members/:id/voucher',
        route(async (req, res) => {
          const id = str(req.params.id);
          const body = (req.body ?? {}) as Record<string, unknown>;
          const code = str(body.code).trim();
          if (!code) {
            res.status(400).json({ error: 'code is required' });
            return;
          }
          const rows = await query(
            `INSERT INTO ${appTable('vouchers')} (customer_id, code, amount, note, created_by)
             VALUES ($1, $2, $3, $4, $5) RETURNING *`,
            [id, code, optNumber(body.amount), str(body.note) || null, actor(req)]
          );
          res.status(201).json(rows[0]);
        })
      );

      app.post(
        '/api/members/:id/ticket',
        route(async (req, res) => {
          const id = str(req.params.id);
          const body = (req.body ?? {}) as Record<string, unknown>;
          const subject = str(body.subject).trim();
          if (!subject) {
            res.status(400).json({ error: 'subject is required' });
            return;
          }
          const rows = await query(
            `INSERT INTO ${appTable('service_tickets')} (customer_id, category, priority, subject, status)
             VALUES ($1, $2, $3, $4, coalesce($5, 'OPEN')) RETURNING *`,
            [id, str(body.category) || null, str(body.priority) || null, subject, str(body.status) || null]
          );
          res.status(201).json(rows[0]);
        })
      );

      app.post(
        '/api/members/:id/tier-change',
        route(async (req, res) => {
          const id = str(req.params.id);
          const body = (req.body ?? {}) as Record<string, unknown>;
          const newTier = str(body.new_tier).trim();
          if (!newTier) {
            res.status(400).json({ error: 'new_tier is required' });
            return;
          }
          const rows = await query(
            `INSERT INTO ${appTable('tier_changes')} (customer_id, old_tier, new_tier, reason)
             VALUES ($1, $2, $3, $4) RETURNING *`,
            [id, str(body.old_tier) || null, newTier, str(body.reason) || null]
          );
          res.status(201).json(rows[0]);
        })
      );

      app.post(
        '/api/members/:id/note',
        route(async (req, res) => {
          const id = str(req.params.id);
          const body = (req.body ?? {}) as Record<string, unknown>;
          const text = str(body.body).trim();
          if (!text) {
            res.status(400).json({ error: 'body is required' });
            return;
          }
          const rows = await query(
            `INSERT INTO ${appTable('notes')} (customer_id, body, author)
             VALUES ($1, $2, $3) RETURNING *`,
            [id, text, actor(req)]
          );
          res.status(201).json(rows[0]);
        })
      );

      // Goodwill / clawback points adjustment (signed integer).
      app.post(
        '/api/members/:id/points',
        route(async (req, res) => {
          const id = str(req.params.id);
          const body = (req.body ?? {}) as Record<string, unknown>;
          const points = optNumber(body.points);
          if (points === null || !Number.isInteger(points) || points === 0) {
            res.status(400).json({ error: 'points must be a non-zero integer' });
            return;
          }
          const rows = await query(
            `INSERT INTO ${appTable('points_adjustments')} (customer_id, points, reason, created_by)
             VALUES ($1, $2, $3, $4) RETURNING *`,
            [id, points, str(body.reason) || null, actor(req)]
          );
          res.status(201).json(rows[0]);
        })
      );

      // Redeem a catalog reward on the member's behalf.
      app.post(
        '/api/members/:id/redeem',
        route(async (req, res) => {
          const id = str(req.params.id);
          const body = (req.body ?? {}) as Record<string, unknown>;
          const rewardId = str(body.reward_id).trim();
          if (!rewardId) {
            res.status(400).json({ error: 'reward_id is required' });
            return;
          }
          const rows = await query(
            `INSERT INTO ${appTable('reward_grants')} (customer_id, reward_id, reward_name, point_cost, created_by)
             VALUES ($1, $2, $3, $4, $5) RETURNING *`,
            [id, rewardId, str(body.reward_name) || null, optNumber(body.point_cost), actor(req)]
          );
          res.status(201).json(rows[0]);
        })
      );

      // ── Rewards catalog performance ───────────────────────────────────────
      app.get(
        '/api/rewards',
        route(async (_req, res) => {
          const [kpis, rows] = await Promise.all([
            query(
              `SELECT count(*)::int AS rewards,
                      sum(redemptions)::int AS redemptions,
                      sum(points_spent)::bigint AS points_spent,
                      round(sum(value_delivered_php)::numeric, 0)::float8 AS value_delivered_php
               FROM ${REWARDS}`
            ),
            query(
              `SELECT reward_id, reward_name, category, point_cost::int AS point_cost,
                      est_value_php::float8 AS est_value_php, reward_tier,
                      value_per_point::float8 AS value_per_point, is_active,
                      redemptions::int AS redemptions, points_spent::bigint AS points_spent,
                      unique_members::int AS unique_members,
                      last_redeemed_date::text AS last_redeemed_date,
                      value_delivered_php::float8 AS value_delivered_php,
                      pct_of_redemptions::float8 AS pct_of_redemptions
               FROM ${REWARDS}
               ORDER BY redemptions DESC NULLS LAST, point_cost`
            ),
          ]);
          res.json({ kpis: kpis[0] ?? null, rows });
        })
      );

      // ── Stores overview ───────────────────────────────────────────────────
      app.get(
        '/api/stores',
        route(async (_req, res) => {
          const [kpis, top, rows] = await Promise.all([
            query(
              `SELECT count(*)::int AS stores,
                      round(sum(revenue)::numeric, 0)::float8 AS revenue,
                      sum(orders)::int AS orders,
                      round(avg(drive_thru_share)::numeric, 4)::float8 AS avg_drive_thru_share,
                      count(*) FILTER (WHERE has_drive_thru)::int AS drive_thru_stores,
                      count(*) FILTER (WHERE has_mccafe)::int AS mccafe_stores
               FROM ${STORES}`
            ),
            query(
              `SELECT store_name, region, round(revenue::numeric, 0)::float8 AS revenue
               FROM ${STORES} ORDER BY revenue DESC NULLS LAST LIMIT 12`
            ),
            query(
              `SELECT store_id, store_name, region, province, city, store_format,
                      has_drive_thru, has_mccafe, orders::int AS orders,
                      round(revenue::numeric, 0)::float8 AS revenue,
                      round(avg_order_value::numeric, 2)::float8 AS avg_order_value,
                      unique_customers::int AS unique_customers,
                      round(drive_thru_share::numeric, 4)::float8 AS drive_thru_share,
                      round(revenue_per_customer::numeric, 2)::float8 AS revenue_per_customer
               FROM ${STORES} ORDER BY revenue DESC NULLS LAST`
            ),
          ]);
          res.json({ kpis: kpis[0] ?? null, top, rows });
        })
      );
    });
  },
}).catch(console.error);
