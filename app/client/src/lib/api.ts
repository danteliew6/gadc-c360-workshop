import { useCallback, useEffect, useRef, useState } from 'react';

export interface AsyncState<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  reload: () => void;
}

async function fetchJson<T>(url: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(url, { signal });
  if (!res.ok) {
    let message = `Request failed (${res.status})`;
    try {
      const body = (await res.json()) as { error?: string };
      if (body?.error) message = body.error;
    } catch {
      /* non-JSON error body */
    }
    throw new Error(message);
  }
  return (await res.json()) as T;
}

/** POST a JSON body and return the parsed JSON response (throws on non-2xx). */
export async function postJson<T>(url: string, body: unknown): Promise<T> {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    let message = `Request failed (${res.status})`;
    try {
      const b = (await res.json()) as { error?: string };
      if (b?.error) message = b.error;
    } catch {
      /* non-JSON error body */
    }
    throw new Error(message);
  }
  return (await res.json()) as T;
}

/**
 * Fetch JSON from an API route. Re-fetches when `url` changes (pass `null` to
 * skip). `refreshMs` polls on an interval (used by the live overview).
 */
export function useApi<T>(url: string | null, refreshMs?: number): AsyncState<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState<boolean>(url !== null);
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(0);
  const isFirst = useRef(true);

  const reload = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    if (url === null) {
      // Deliberate reset when the caller disables the query (url flips to null);
      // the rule's cascading-render concern doesn't apply here.
      /* eslint-disable react-hooks/set-state-in-effect */
      setData(null);
      setLoading(false);
      setError(null);
      /* eslint-enable react-hooks/set-state-in-effect */
      return;
    }
    const controller = new AbortController();
    // Keep stale data visible while refetching; only show the skeleton first time.
    if (isFirst.current) setLoading(true);
    fetchJson<T>(url, controller.signal)
      .then((d) => {
        setData(d);
        setError(null);
      })
      .catch((err: unknown) => {
        if (controller.signal.aborted) return;
        setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setLoading(false);
          isFirst.current = false;
        }
      });
    return () => controller.abort();
  }, [url, tick]);

  useEffect(() => {
    if (url === null || !refreshMs) return;
    const id = setInterval(reload, refreshMs);
    return () => clearInterval(id);
  }, [url, refreshMs, reload]);

  return { data, loading, error, reload };
}
