import { COLORS } from '../lib/colors';

export type ChipTone = keyof typeof COLORS;

/**
 * Small semantic label pill (churn band, loyalty tier, RFM segment). Uses a
 * tinted background derived from the tone so it reads as a status, not chrome.
 */
export function Chip({ label, tone = 'muted' }: { label: string; tone?: ChipTone }) {
  const color = COLORS[tone];
  return (
    <span
      className="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium whitespace-nowrap"
      style={{ color, backgroundColor: `${color}1f`, border: `1px solid ${color}40` }}
    >
      {label}
    </span>
  );
}
