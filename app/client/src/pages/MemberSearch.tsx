import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router';
import {
  Input,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@databricks/appkit-ui/react';
import { Search } from 'lucide-react';
import { Panel } from '../components/Panel';
import { StateBlock } from '../components/StateBlock';
import { DataGrid, type Column } from '../components/DataGrid';
import { Chip } from '../components/Chip';
import { useApi } from '../lib/api';
import { CHURN_TONE, TIER_TONE } from '../lib/colors';
import { php, int, ymd } from '../lib/format';
import type { Facets, MemberSearchRow } from '../lib/types';

const ALL = '__all__';

export function MemberSearch() {
  const navigate = useNavigate();
  const [q, setQ] = useState('');
  const [dq, setDq] = useState('');
  const [region, setRegion] = useState(ALL);
  const [segment, setSegment] = useState(ALL);
  const [tier, setTier] = useState(ALL);

  // Debounce the free-text query so we don't refetch on every keystroke.
  useEffect(() => {
    const id = setTimeout(() => setDq(q.trim()), 300);
    return () => clearTimeout(id);
  }, [q]);

  const facets = useApi<Facets>('/api/members/facets');
  const url = useMemo(() => {
    const params = new URLSearchParams({ limit: '50' });
    if (dq) params.set('q', dq);
    if (region !== ALL) params.set('region', region);
    if (segment !== ALL) params.set('segment', segment);
    if (tier !== ALL) params.set('tier', tier);
    return `/api/members?${params.toString()}`;
  }, [dq, region, segment, tier]);
  const members = useApi<MemberSearchRow[]>(url);

  const columns: Column<MemberSearchRow>[] = [
    { key: 'customer_id', label: 'Customer ID' },
    { key: 'full_name', label: 'Name' },
    { key: 'region', label: 'Region' },
    {
      key: 'current_tier',
      label: 'Tier',
      render: (v) => (v ? <Chip label={v as string} tone={TIER_TONE[v as string] ?? 'gold'} /> : '—'),
    },
    { key: 'points_balance', label: 'Points', align: 'right', render: (v) => int(v) },
    {
      key: 'rfm_segment',
      label: 'Segment',
      render: (v) => (v ? <Chip label={v as string} tone="info" /> : '—'),
    },
    {
      key: 'churn_risk',
      label: 'Churn',
      render: (v) => {
        const band = ((v as string | null) ?? '').toUpperCase();
        return v ? <Chip label={band} tone={CHURN_TONE[band] ?? 'muted'} /> : '—';
      },
    },
    { key: 'total_orders', label: 'Orders', align: 'right', render: (v) => int(v) },
    { key: 'total_spend', label: 'Total spend', align: 'right', render: (v) => php(v) },
    { key: 'last_order_date', label: 'Last order', render: (v) => ymd(v as string) },
  ];

  return (
    <div className="space-y-4">
      <Panel
        title="Find a member"
        subtitle="Search by name, customer ID, or email · filter by region, tier, and RFM segment"
      >
        <div className="flex flex-col gap-3 md:flex-row md:items-center">
          <div className="relative flex-1 min-w-[220px]">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Name, customer ID, or email…"
              className="pl-9"
              aria-label="Search members"
            />
          </div>
          <Select value={region} onValueChange={setRegion}>
            <SelectTrigger className="md:w-52" aria-label="Filter by region">
              <SelectValue placeholder="Region" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>All regions</SelectItem>
              {(facets.data?.regions ?? []).map((r) => (
                <SelectItem key={r} value={r}>
                  {r}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={tier} onValueChange={setTier}>
            <SelectTrigger className="md:w-44" aria-label="Filter by tier">
              <SelectValue placeholder="Tier" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>All tiers</SelectItem>
              {(facets.data?.tiers ?? []).map((t) => (
                <SelectItem key={t} value={t}>
                  {t}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={segment} onValueChange={setSegment}>
            <SelectTrigger className="md:w-52" aria-label="Filter by segment">
              <SelectValue placeholder="Segment" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>All segments</SelectItem>
              {(facets.data?.segments ?? []).map((s) => (
                <SelectItem key={s} value={s}>
                  {s}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </Panel>

      <Panel
        title="Members"
        subtitle={members.data ? `${members.data.length} match${members.data.length === 1 ? '' : 'es'} · click a row to open the profile` : 'Loading…'}
      >
        <StateBlock
          loading={members.loading}
          error={members.error}
          empty={!(members.data ?? []).length}
          emptyTitle="No members found"
          emptyMessage="Try a broader search or clear the region / segment filters."
          height={320}
        >
          <DataGrid
            rows={members.data ?? []}
            columns={columns}
            pageSize={12}
            onRowClick={(row) => void navigate(`/members/${encodeURIComponent(row.customer_id)}`)}
          />
        </StateBlock>
      </Panel>
    </div>
  );
}
