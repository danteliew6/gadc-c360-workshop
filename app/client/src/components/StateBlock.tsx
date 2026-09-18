import {
  Alert,
  AlertDescription,
  AlertTitle,
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
  Skeleton,
} from '@databricks/appkit-ui/react';
import { TriangleAlert } from 'lucide-react';

/**
 * Uniform loading / error / empty handling for every data view.
 * Renders `children` only when data is present and non-empty.
 */
export function StateBlock({
  loading,
  error,
  empty,
  emptyTitle = 'No data',
  emptyMessage,
  height = 300,
  children,
}: {
  loading: boolean;
  error: string | null;
  empty?: boolean;
  emptyTitle?: string;
  emptyMessage?: string;
  height?: number;
  children: React.ReactNode;
}) {
  if (error) {
    return (
      <Alert variant="destructive">
        <TriangleAlert className="h-4 w-4" />
        <AlertTitle>Couldn’t load data</AlertTitle>
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    );
  }
  if (loading) {
    return <Skeleton className="w-full rounded-md" style={{ height }} />;
  }
  if (empty) {
    return (
      <Empty>
        <EmptyHeader>
          <EmptyTitle>{emptyTitle}</EmptyTitle>
          {emptyMessage ? <EmptyDescription>{emptyMessage}</EmptyDescription> : null}
        </EmptyHeader>
      </Empty>
    );
  }
  return <>{children}</>;
}
