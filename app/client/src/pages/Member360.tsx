import { useState } from 'react';
import { useNavigate, useParams } from 'react-router';
import {
  Alert,
  AlertDescription,
  Button,
  Input,
  Label,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Separator,
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
  Textarea,
} from '@databricks/appkit-ui/react';
import { ArrowLeft, Gift, LifeBuoy, Crown, StickyNote, CheckCircle2 } from 'lucide-react';
import { KpiCard, KpiRow } from '../components/KpiCard';
import { Panel } from '../components/Panel';
import { StateBlock } from '../components/StateBlock';
import { Chip } from '../components/Chip';
import { useApi, postJson } from '../lib/api';
import { CHURN_TONE, TIER_ORDER } from '../lib/colors';
import { php, int, dec, pct, ymd } from '../lib/format';
import type { ActionEntry, Member, MemberDetail } from '../lib/types';

// ── Small building blocks ──────────────────────────────────────────────────

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="text-sm font-medium">{children}</div>
    </div>
  );
}

function churnTone(band: string | null | undefined) {
  return CHURN_TONE[String(band ?? '').toUpperCase()] ?? 'muted';
}

const ACTION_META: Record<ActionEntry['kind'], { label: string; tone: 'gold' | 'info' | 'brand' | 'muted' }> = {
  voucher: { label: 'Voucher', tone: 'gold' },
  ticket: { label: 'Ticket', tone: 'info' },
  tier_change: { label: 'Tier', tone: 'brand' },
  note: { label: 'Note', tone: 'muted' },
};

// ── Action forms (write-back into app.*) ─────────────────────────────────────

function FormFeedback({ error, ok }: { error: string | null; ok: string | null }) {
  if (error)
    return (
      <Alert variant="destructive" className="mt-3">
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    );
  if (ok)
    return (
      <div className="mt-3 flex items-center gap-2 text-sm text-[color:var(--success)]">
        <CheckCircle2 className="h-4 w-4" /> {ok}
      </div>
    );
  return null;
}

function useSubmit(onDone: () => void) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [ok, setOk] = useState<string | null>(null);
  async function run(fn: () => Promise<void>, okMsg: string) {
    setBusy(true);
    setError(null);
    setOk(null);
    try {
      await fn();
      setOk(okMsg);
      onDone();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }
  return { busy, error, ok, run };
}

function VoucherForm({ id, onDone }: { id: string; onDone: () => void }) {
  const [code, setCode] = useState('');
  const [amount, setAmount] = useState('');
  const [note, setNote] = useState('');
  const { busy, error, ok, run } = useSubmit(onDone);
  return (
    <form
      className="space-y-3"
      onSubmit={(e) => {
        e.preventDefault();
        void run(async () => {
          await postJson(`/api/members/${encodeURIComponent(id)}/voucher`, {
            code,
            amount: amount === '' ? null : Number(amount),
            note,
          });
          setCode('');
          setAmount('');
          setNote('');
        }, 'Voucher issued.');
      }}
    >
      <div className="space-y-1.5">
        <Label htmlFor="v-code">Voucher code</Label>
        <Input id="v-code" value={code} onChange={(e) => setCode(e.target.value)} placeholder="MCDO-PH-50" required />
      </div>
      <div className="space-y-1.5">
        <Label htmlFor="v-amount">Amount (₱)</Label>
        <Input id="v-amount" type="number" step="0.01" value={amount} onChange={(e) => setAmount(e.target.value)} placeholder="50.00" />
      </div>
      <div className="space-y-1.5">
        <Label htmlFor="v-note">Note</Label>
        <Input id="v-note" value={note} onChange={(e) => setNote(e.target.value)} placeholder="Goodwill gesture" />
      </div>
      <Button type="submit" disabled={busy || !code.trim()}>
        {busy ? 'Issuing…' : 'Issue voucher'}
      </Button>
      <FormFeedback error={error} ok={ok} />
    </form>
  );
}

function TicketForm({ id, onDone }: { id: string; onDone: () => void }) {
  const [category, setCategory] = useState('DELIVERY');
  const [priority, setPriority] = useState('MEDIUM');
  const [subject, setSubject] = useState('');
  const { busy, error, ok, run } = useSubmit(onDone);
  return (
    <form
      className="space-y-3"
      onSubmit={(e) => {
        e.preventDefault();
        void run(async () => {
          await postJson(`/api/members/${encodeURIComponent(id)}/ticket`, { category, priority, subject });
          setSubject('');
        }, 'Ticket created.');
      }}
    >
      <div className="grid grid-cols-2 gap-3">
        <div className="space-y-1.5">
          <Label>Category</Label>
          <Select value={category} onValueChange={setCategory}>
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {['DELIVERY', 'APP', 'IN_STORE', 'REWARDS', 'BILLING', 'OTHER'].map((c) => (
                <SelectItem key={c} value={c}>
                  {c}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-1.5">
          <Label>Priority</Label>
          <Select value={priority} onValueChange={setPriority}>
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {['LOW', 'MEDIUM', 'HIGH', 'URGENT'].map((p) => (
                <SelectItem key={p} value={p}>
                  {p}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
      <div className="space-y-1.5">
        <Label htmlFor="t-subject">Subject</Label>
        <Input id="t-subject" value={subject} onChange={(e) => setSubject(e.target.value)} placeholder="Late delivery, missing item…" required />
      </div>
      <Button type="submit" disabled={busy || !subject.trim()}>
        {busy ? 'Creating…' : 'Create ticket'}
      </Button>
      <FormFeedback error={error} ok={ok} />
    </form>
  );
}

function TierForm({ id, member, onDone }: { id: string; member: Member; onDone: () => void }) {
  const [newTier, setNewTier] = useState('');
  const [reason, setReason] = useState('');
  const { busy, error, ok, run } = useSubmit(onDone);
  return (
    <form
      className="space-y-3"
      onSubmit={(e) => {
        e.preventDefault();
        void run(async () => {
          await postJson(`/api/members/${encodeURIComponent(id)}/tier-change`, {
            old_tier: member.loyalty_tier,
            new_tier: newTier,
            reason,
          });
          setNewTier('');
          setReason('');
        }, 'Tier change recorded.');
      }}
    >
      <Field label="Current tier">{member.loyalty_tier ?? '—'}</Field>
      <div className="space-y-1.5">
        <Label>New tier</Label>
        <Select value={newTier} onValueChange={setNewTier}>
          <SelectTrigger>
            <SelectValue placeholder="Select tier…" />
          </SelectTrigger>
          <SelectContent>
            {TIER_ORDER.map((t) => (
              <SelectItem key={t} value={t}>
                {t}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="space-y-1.5">
        <Label htmlFor="tc-reason">Reason</Label>
        <Input id="tc-reason" value={reason} onChange={(e) => setReason(e.target.value)} placeholder="Retention offer, service recovery…" />
      </div>
      <Button type="submit" disabled={busy || !newTier}>
        {busy ? 'Saving…' : 'Record tier change'}
      </Button>
      <FormFeedback error={error} ok={ok} />
    </form>
  );
}

function NoteForm({ id, onDone }: { id: string; onDone: () => void }) {
  const [body, setBody] = useState('');
  const { busy, error, ok, run } = useSubmit(onDone);
  return (
    <form
      className="space-y-3"
      onSubmit={(e) => {
        e.preventDefault();
        void run(async () => {
          await postJson(`/api/members/${encodeURIComponent(id)}/note`, { body });
          setBody('');
        }, 'Note saved.');
      }}
    >
      <div className="space-y-1.5">
        <Label htmlFor="n-body">Note</Label>
        <Textarea id="n-body" value={body} onChange={(e) => setBody(e.target.value)} placeholder="Internal note about this member…" rows={4} required />
      </div>
      <Button type="submit" disabled={busy || !body.trim()}>
        {busy ? 'Saving…' : 'Save note'}
      </Button>
      <FormFeedback error={error} ok={ok} />
    </form>
  );
}

// ── Page ─────────────────────────────────────────────────────────────────────

export function Member360() {
  const { id } = useParams();
  const navigate = useNavigate();
  const detail = useApi<MemberDetail>(id ? `/api/members/${encodeURIComponent(id)}` : null);
  const m = detail.data?.member;

  return (
    <div className="space-y-4">
      <Button variant="ghost" size="sm" className="-ml-2" onClick={() => void navigate('/')}>
        <ArrowLeft className="h-4 w-4 mr-1.5" /> Back to search
      </Button>

      <StateBlock
        loading={detail.loading}
        error={detail.error}
        empty={!m}
        emptyTitle="Member not found"
        emptyMessage="No customer_360 record matched that ID."
        height={200}
      >
        {m ? (
          <div className="space-y-4">
            {/* Header */}
            <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
              <h2 className="text-2xl font-bold">{m.full_name}</h2>
              <div className="flex flex-wrap items-center gap-2">
                {m.loyalty_tier ? <Chip label={m.loyalty_tier} tone="gold" /> : null}
                {m.rfm_segment ? <Chip label={m.rfm_segment} tone="info" /> : null}
                {m.churn_risk ? (
                  <Chip label={`Churn: ${m.churn_risk}`} tone={churnTone(m.churn_risk)} />
                ) : null}
              </div>
              <div className="text-sm text-muted-foreground">
                {m.customer_id}
                {m.city || m.region ? ` · ${[m.city, m.region].filter(Boolean).join(', ')}` : ''}
              </div>
            </div>

            {/* KPI row */}
            <KpiRow>
              <KpiCard label="Lifetime spend" value={php(m.total_spend)} tone="gold" />
              <KpiCard label="Total orders" value={int(m.total_orders)} tone="info" />
              <KpiCard label="Avg order value" value={php(m.avg_order_value)} tone="info" />
              <KpiCard label="Est. CLV" value={php(m.clv_estimate)} tone="good" />
              <KpiCard
                label="Recency"
                value={`${int(m.recency_days)} d`}
                sub="since last order"
                tone={m.recency_days > 90 ? 'bad' : 'text'}
              />
              <KpiCard label="Engagement" value={dec(m.engagement_score, 1)} sub="score" tone="brand" />
            </KpiRow>

            <div className="grid gap-4 lg:grid-cols-3">
              {/* Profile detail (2 cols) */}
              <div className="space-y-4 lg:col-span-2">
                <Panel title="RFM & churn" subtitle="Recency / Frequency / Monetary scoring">
                  <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                    <Field label="RFM score">{m.rfm_score ?? '—'}</Field>
                    <Field label="R / F / M">{`${m.r_score ?? '–'} / ${m.f_score ?? '–'} / ${m.m_score ?? '–'}`}</Field>
                    <Field label="Segment">{m.rfm_segment ?? '—'}</Field>
                    <Field label="Churn risk">
                      {m.churn_risk ? <Chip label={m.churn_risk} tone={churnTone(m.churn_risk)} /> : '—'}
                    </Field>
                  </div>
                </Panel>

                <Panel title="Favorites & channels">
                  <div className="grid grid-cols-2 gap-4 sm:grid-cols-3">
                    <Field label="Favorite store">{m.favorite_store ?? '—'}</Field>
                    <Field label="Favorite category">{m.favorite_category ?? '—'}</Field>
                    <Field label="Preferred channel">{m.preferred_channel ?? '—'}</Field>
                    <Field label="Signup channel">{m.signup_channel ?? '—'}</Field>
                    <Field label="Distinct stores">{int(m.distinct_stores)}</Field>
                    <Field label="Delivery share">{pct(m.delivery_share)}</Field>
                  </div>
                </Panel>

                <Panel title="Engagement & tenure">
                  <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                    <Field label="App events">{int(m.total_app_events)}</Field>
                    <Field label="Rewards redeemed">{int(m.rewards_redeemed)}</Field>
                    <Field label="Active days">{int(m.active_days)}</Field>
                    <Field label="Tenure">{`${int(m.tenure_days)} d`}</Field>
                    <Field label="Promo orders">{int(m.promo_orders)}</Field>
                    <Field label="Delivery orders">{int(m.delivery_orders)}</Field>
                    <Field label="Last active">{ymd(m.last_active_date)}</Field>
                    <Field label="Member since">{ymd(m.signup_date)}</Field>
                  </div>
                </Panel>

                <Panel title="Contact & consent">
                  <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                    <Field label="Email">{m.email ?? '—'}</Field>
                    <Field label="Mobile">{m.mobile ?? '—'}</Field>
                    <Field label="Language">{m.preferred_language ?? '—'}</Field>
                    <Field label="Marketing consent">{m.marketing_consent ? 'Yes' : 'No'}</Field>
                    <Field label="Age">{m.age != null ? int(m.age) : '—'}</Field>
                    <Field label="Gender">{m.gender ?? '—'}</Field>
                    <Field label="Province">{m.province ?? '—'}</Field>
                    <Field label="Birth date">{ymd(m.birth_date)}</Field>
                  </div>
                </Panel>
              </div>

              {/* Actions + feed (1 col) */}
              <div className="space-y-4">
                <Panel title="Take action" subtitle="Writes to the app service layer (app.* tables)">
                  <Tabs defaultValue="voucher">
                    <TabsList className="grid w-full grid-cols-4">
                      <TabsTrigger value="voucher" aria-label="Voucher">
                        <Gift className="h-4 w-4" />
                      </TabsTrigger>
                      <TabsTrigger value="ticket" aria-label="Ticket">
                        <LifeBuoy className="h-4 w-4" />
                      </TabsTrigger>
                      <TabsTrigger value="tier" aria-label="Tier change">
                        <Crown className="h-4 w-4" />
                      </TabsTrigger>
                      <TabsTrigger value="note" aria-label="Note">
                        <StickyNote className="h-4 w-4" />
                      </TabsTrigger>
                    </TabsList>
                    <TabsContent value="voucher" className="pt-3">
                      <VoucherForm id={m.customer_id} onDone={detail.reload} />
                    </TabsContent>
                    <TabsContent value="ticket" className="pt-3">
                      <TicketForm id={m.customer_id} onDone={detail.reload} />
                    </TabsContent>
                    <TabsContent value="tier" className="pt-3">
                      <TierForm id={m.customer_id} member={m} onDone={detail.reload} />
                    </TabsContent>
                    <TabsContent value="note" className="pt-3">
                      <NoteForm id={m.customer_id} onDone={detail.reload} />
                    </TabsContent>
                  </Tabs>
                </Panel>

                <Panel title="Recent actions" subtitle="Latest write-back for this member">
                  {(detail.data?.actions ?? []).length ? (
                    <ul className="space-y-3">
                      {detail.data!.actions.map((a, i) => {
                        const meta = ACTION_META[a.kind];
                        return (
                          <li key={`${a.created_at}-${a.kind}-${a.summary}`}>
                            <div className="flex items-center gap-2">
                              <Chip label={meta.label} tone={meta.tone} />
                              <span className="text-xs text-muted-foreground">{ymd(a.created_at)}</span>
                            </div>
                            <div className="text-sm mt-1">{a.summary}</div>
                            {i < detail.data!.actions.length - 1 ? <Separator className="mt-3" /> : null}
                          </li>
                        );
                      })}
                    </ul>
                  ) : (
                    <div className="text-sm text-muted-foreground py-4 text-center">
                      No actions recorded yet.
                    </div>
                  )}
                </Panel>
              </div>
            </div>
          </div>
        ) : null}
      </StateBlock>
    </div>
  );
}
