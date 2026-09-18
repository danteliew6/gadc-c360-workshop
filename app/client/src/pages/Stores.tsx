import { BarChart } from '@databricks/appkit-ui/react';
import { KpiCard, KpiRow } from '../components/KpiCard';
import { Panel } from '../components/Panel';
import { StateBlock } from '../components/StateBlock';
import { DataGrid, type Column } from '../components/DataGrid';
import { useApi } from '../lib/api';
import { COLORS } from '../lib/colors';
import { int, php, phpCompact, pct } from '../lib/format';
import type { StoreRow, StoresResponse } from '../lib/types';

export function Stores() {
  const stores = useApi<StoresResponse>('/api/stores');
  const k = stores.data?.kpis;
  const top = stores.data?.top ?? [];
  const rows = stores.data?.rows ?? [];

  const columns: Column<StoreRow>[] = [
    { key: 'store_name', label: 'Store' },
    { key: 'region', label: 'Region' },
    { key: 'city', label: 'City' },
    { key: 'store_format', label: 'Format' },
    { key: 'has_drive_thru', label: 'Drive-thru', render: (v) => (v ? 'Yes' : 'No') },
    { key: 'has_mccafe', label: 'McCafé', render: (v) => (v ? 'Yes' : 'No') },
    { key: 'orders', label: 'Orders', align: 'right', render: (v) => int(v) },
    { key: 'revenue', label: 'Revenue', align: 'right', render: (v) => phpCompact(v) },
    { key: 'avg_order_value', label: 'AOV', align: 'right', render: (v) => php(v) },
    { key: 'unique_customers', label: 'Customers', align: 'right', render: (v) => int(v) },
    { key: 'drive_thru_share', label: 'DT share', align: 'right', render: (v) => pct(v) },
    { key: 'revenue_per_customer', label: 'Rev / cust', align: 'right', render: (v) => php(v) },
  ];

  return (
    <div className="space-y-4">
      <StateBlock loading={stores.loading} error={stores.error} empty={!k} height={120}>
        {k ? (
          <KpiRow>
            <KpiCard label="Stores" value={int(k.stores)} tone="brand" />
            <KpiCard label="Total revenue" value={phpCompact(k.revenue)} tone="gold" />
            <KpiCard label="Total orders" value={int(k.orders)} tone="info" />
            <KpiCard label="Avg drive-thru share" value={pct(k.avg_drive_thru_share)} tone="info" />
            <KpiCard label="Drive-thru stores" value={int(k.drive_thru_stores)} tone="good" />
            <KpiCard label="McCafé stores" value={int(k.mccafe_stores)} tone="good" />
          </KpiRow>
        ) : null}
      </StateBlock>

      <Panel title="Top stores by revenue" subtitle="Highest-grossing locations across the network">
        <StateBlock loading={stores.loading} error={stores.error} empty={!top.length} height={360}>
          <BarChart
            data={top}
            xKey="store_name"
            yKey="revenue"
            orientation="horizontal"
            colors={[COLORS.gold]}
            height={360}
            showLegend={false}
          />
        </StateBlock>
      </Panel>

      <Panel title="All stores" subtitle={`${rows.length} locations · sortable`}>
        <StateBlock loading={stores.loading} error={stores.error} empty={!rows.length} height={320}>
          <DataGrid rows={rows} columns={columns} pageSize={15} initialSort={{ key: 'revenue', dir: 'desc' }} />
        </StateBlock>
      </Panel>
    </div>
  );
}
