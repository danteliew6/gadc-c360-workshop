import { Card, CardContent } from '@databricks/appkit-ui/react';
import { COLORS } from '../lib/colors';

export type Tone = keyof typeof COLORS;

export function KpiCard({
  label,
  value,
  sub,
  tone = 'text',
}: {
  label: string;
  value: string;
  sub?: string;
  tone?: Tone;
}) {
  const color = COLORS[tone];
  return (
    <Card className="flex-1 min-w-[150px]">
      <CardContent className="py-3.5 px-4">
        <div className="text-xs text-muted-foreground">{label}</div>
        <div className="text-2xl font-bold leading-tight mt-0.5" style={{ color }}>
          {value}
        </div>
        {sub ? <div className="text-[0.72rem] text-muted-foreground mt-0.5">{sub}</div> : null}
      </CardContent>
    </Card>
  );
}

export function KpiRow({ children }: { children: React.ReactNode }) {
  return <div className="flex flex-wrap gap-3 mb-5">{children}</div>;
}
