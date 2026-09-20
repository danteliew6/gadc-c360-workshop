import { BarChart } from '@databricks/appkit-ui/react';
import { KpiCard, KpiRow } from '../components/KpiCard';
import { Panel } from '../components/Panel';
import { StateBlock } from '../components/StateBlock';
import { DataGrid, type Column } from '../components/DataGrid';
import { Chip } from '../components/Chip';
import { useApi } from '../lib/api';
import { COLORS } from '../lib/colors';
import { int, php, phpCompact, pct } from '../lib/format';
import type { RewardPerf, RewardsResponse } from '../lib/types';

export function Rewards() {
  const rewards = useApi<RewardsResponse>('/api/rewards');
  const k = rewards.data?.kpis;
  const rows = rewards.data?.rows ?? [];
  const top = rows.slice(0, 12).map((r) => ({ reward_name: r.reward_name, redemptions: r.redemptions }));

  const columns: Column<RewardPerf>[] = [
    { key: 'reward_name', label: 'Reward' },
    {
      key: 'reward_tier',
      label: 'Tier',
      render: (v) => (v ? <Chip label={v as string} tone="gold" /> : '—'),
    },
    { key: 'category', label: 'Category' },
    { key: 'point_cost', label: 'Point cost', align: 'right', render: (v) => int(v) },
    { key: 'est_value_php', label: 'Value', align: 'right', render: (v) => php(v) },
    { key: 'redemptions', label: 'Redemptions', align: 'right', render: (v) => int(v) },
    { key: 'unique_members', label: 'Members', align: 'right', render: (v) => int(v) },
    { key: 'points_spent', label: 'Points spent', align: 'right', render: (v) => int(v) },
    { key: 'value_delivered_php', label: 'Value delivered', align: 'right', render: (v) => phpCompact(v) },
    { key: 'pct_of_redemptions', label: '% of redemptions', align: 'right', render: (v) => pct(v) },
  ];

  return (
    <div className="space-y-4">
      <StateBlock loading={rewards.loading} error={rewards.error} empty={!k} height={120}>
        {k ? (
          <KpiRow>
            <KpiCard label="Active rewards" value={int(k.rewards)} tone="brand" />
            <KpiCard label="Total redemptions" value={int(k.redemptions)} tone="gold" />
            <KpiCard label="Points redeemed" value={int(k.points_spent)} tone="info" />
            <KpiCard label="Value delivered" value={phpCompact(k.value_delivered_php)} tone="good" />
          </KpiRow>
        ) : null}
      </StateBlock>

      <Panel title="Most-redeemed rewards" subtitle="Redemption volume across the MyMcDonald's Rewards catalog">
        <StateBlock loading={rewards.loading} error={rewards.error} empty={!top.length} height={360}>
          <BarChart
            data={top}
            xKey="reward_name"
            yKey="redemptions"
            orientation="horizontal"
            colors={[COLORS.gold]}
            height={360}
            showLegend={false}
          />
        </StateBlock>
      </Panel>

      <Panel title="Rewards catalog" subtitle={`${rows.length} rewards · sortable`}>
        <StateBlock loading={rewards.loading} error={rewards.error} empty={!rows.length} height={320}>
          <DataGrid rows={rows} columns={columns} pageSize={15} initialSort={{ key: 'redemptions', dir: 'desc' }} />
        </StateBlock>
      </Panel>
    </div>
  );
}
