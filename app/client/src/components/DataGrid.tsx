import { useMemo, useState } from 'react';
import {
  Button,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@databricks/appkit-ui/react';
import { ArrowDown, ArrowUp, ChevronsUpDown } from 'lucide-react';
import { prettify } from '../lib/format';

export interface Column<T> {
  key: keyof T & string;
  label?: string;
  align?: 'left' | 'right';
  render?: (value: unknown, row: T) => React.ReactNode;
}

function defaultRender(value: unknown): React.ReactNode {
  if (value === null || value === undefined) return '—';
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'number' || typeof value === 'string') return String(value);
  return JSON.stringify(value);
}

/** Safe string coercion for client-side sorting of untyped cell values. */
function sortText(value: unknown): string {
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  return value == null ? '' : JSON.stringify(value);
}

/**
 * Compact, client-side sortable/paginated table over a bounded result set
 * (the server already sorts + LIMITs). Columns auto-derive from the first row
 * when not provided.
 */
export function DataGrid<T extends object>({
  rows,
  columns,
  pageSize = 12,
  initialSort,
  onRowClick,
}: {
  rows: T[];
  columns?: Column<T>[];
  pageSize?: number;
  initialSort?: { key: keyof T & string; dir: 'asc' | 'desc' };
  onRowClick?: (row: T) => void;
}) {
  const cols: Column<T>[] = useMemo(() => {
    if (columns) return columns;
    const first = rows[0];
    if (!first) return [];
    return Object.keys(first).map((k) => ({ key: k as keyof T & string }));
  }, [columns, rows]);

  const [sort, setSort] = useState<{ key: string; dir: 'asc' | 'desc' } | null>(initialSort ?? null);
  const [page, setPage] = useState(0);

  const sorted = useMemo(() => {
    if (!sort) return rows;
    const { key, dir } = sort;
    const copy = [...rows];
    copy.sort((a, b) => {
      const av = (a as Record<string, unknown>)[key];
      const bv = (b as Record<string, unknown>)[key];
      let cmp: number;
      if (typeof av === 'number' && typeof bv === 'number') cmp = av - bv;
      else if (typeof av === 'boolean' && typeof bv === 'boolean') cmp = Number(av) - Number(bv);
      else cmp = sortText(av).localeCompare(sortText(bv), undefined, { numeric: true });
      return dir === 'asc' ? cmp : -cmp;
    });
    return copy;
  }, [rows, sort]);

  const pageCount = Math.max(1, Math.ceil(sorted.length / pageSize));
  const clampedPage = Math.min(page, pageCount - 1);
  const view = sorted.slice(clampedPage * pageSize, clampedPage * pageSize + pageSize);

  function toggleSort(key: string) {
    setPage(0);
    setSort((s) => {
      if (!s || s.key !== key) return { key, dir: 'desc' };
      if (s.dir === 'desc') return { key, dir: 'asc' };
      return null;
    });
  }

  if (!rows.length) {
    return <div className="text-sm text-muted-foreground py-6 text-center">No rows.</div>;
  }

  return (
    <div className="space-y-3">
      <div className="overflow-x-auto rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              {cols.map((c) => {
                const active = sort?.key === c.key;
                const Icon = !active ? ChevronsUpDown : sort?.dir === 'asc' ? ArrowUp : ArrowDown;
                return (
                  <TableHead
                    key={c.key}
                    className={`cursor-pointer select-none whitespace-nowrap ${c.align === 'right' ? 'text-right' : ''}`}
                    onClick={() => toggleSort(c.key)}
                  >
                    <span className={`inline-flex items-center gap-1 ${c.align === 'right' ? 'flex-row-reverse' : ''}`}>
                      {c.label ?? prettify(c.key)}
                      <Icon className="h-3 w-3 opacity-60" />
                    </span>
                  </TableHead>
                );
              })}
            </TableRow>
          </TableHeader>
          <TableBody>
            {view.map((row, i) => (
              <TableRow
                key={i}
                onClick={onRowClick ? () => onRowClick(row) : undefined}
                className={onRowClick ? 'cursor-pointer' : undefined}
              >
                {cols.map((c) => (
                  <TableCell
                    key={c.key}
                    className={`font-mono text-xs whitespace-nowrap ${c.align === 'right' ? 'text-right' : ''}`}
                  >
                    {c.render ? c.render(row[c.key], row) : defaultRender(row[c.key])}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      {pageCount > 1 ? (
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span>
            {clampedPage * pageSize + 1}–{Math.min(sorted.length, (clampedPage + 1) * pageSize)} of {sorted.length}
          </span>
          <div className="flex gap-2">
            <Button variant="outline" size="sm" disabled={clampedPage === 0} onClick={() => setPage(clampedPage - 1)}>
              Prev
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={clampedPage >= pageCount - 1}
              onClick={() => setPage(clampedPage + 1)}
            >
              Next
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
