/** Display formatters. SQL numerics can arrive as strings (bigint/numeric), so
 * every helper coerces with Number() first. */

export function num(value: unknown): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

/** Integer with thousands separators, e.g. 1,000,000. */
export function int(value: unknown): string {
  return num(value).toLocaleString('en-US', { maximumFractionDigits: 0 });
}

/** Fixed-decimal number with thousands separators. */
export function dec(value: unknown, digits = 1): string {
  return num(value).toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

/** USD, 2 decimals. */
export function usd(value: unknown): string {
  return `$${num(value).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

/** Philippine peso, 2 decimals (e.g. ₱1,234.50). */
export function php(value: unknown, digits = 2): string {
  return `₱${num(value).toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits })}`;
}

/** Compact peso for large figures (e.g. ₱1.2M, ₱48.0K). */
export function phpCompact(value: unknown): string {
  const n = num(value);
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `₱${(n / 1_000_000).toLocaleString('en-US', { maximumFractionDigits: 1 })}M`;
  if (abs >= 1_000) return `₱${(n / 1_000).toLocaleString('en-US', { maximumFractionDigits: 1 })}K`;
  return `₱${n.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
}

/** Percentage from a 0..1 fraction, e.g. 0.975 -> "97.5%". */
export function pct(fraction: unknown, digits = 1): string {
  return `${(num(fraction) * 100).toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits })}%`;
}

/** "HH:MM" from an ISO-ish timestamp string ("2026-06-19 10:05:00"). */
export function hhmm(ts: string | null | undefined): string {
  if (!ts) return '';
  const m = /(\d{2}):(\d{2})/.exec(ts.includes('T') ? ts.split('T')[1] : ts.split(' ')[1] ?? '');
  return m ? `${m[1]}:${m[2]}` : ts;
}

/** Compact date "YYYY-MM-DD" from a timestamp string. */
export function ymd(ts: string | null | undefined): string {
  if (!ts) return '';
  return (ts.includes('T') ? ts.split('T')[0] : ts.split(' ')[0]) ?? ts;
}

/** Prettify a snake_case column/label into "Title Case With Spaces". */
export function prettify(label: string): string {
  return label
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}
