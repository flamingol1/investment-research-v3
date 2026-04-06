/**
 * SSE 采集进度 Hook — 接收 collect_id，返回实时进度状态
 */
import { useState, useEffect, useRef } from 'react';
import type { StepStartEvent, StepCompleteEvent, CollectDoneEvent } from './api';

export interface ProgressStep {
  data_type: string;
  display_name?: string;
  status: 'pending' | 'running' | 'success' | 'failed';
  source?: string;
  records_fetched: number;
  duration_ms: number;
  error: string | null;
}

export type ProgressPhase = 'idle' | 'running' | 'completed' | 'failed';

export interface UseCollectionProgressReturn {
  steps: ProgressStep[];
  currentStep: number;
  totalSteps: number;
  phase: ProgressPhase;
  doneEvent: CollectDoneEvent | null;
  error: string | null;
}

export function useCollectionProgress(collectId: string | null): UseCollectionProgressReturn {
  const [steps, setSteps] = useState<ProgressStep[]>([]);
  const [phase, setPhase] = useState<ProgressPhase>('idle');
  const [doneEvent, setDoneEvent] = useState<CollectDoneEvent | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [totalSteps, setTotalSteps] = useState(0);

  useEffect(() => {
    if (!collectId) return;

    setSteps([]);
    setPhase('running');
    setError(null);
    setDoneEvent(null);
    setTotalSteps(0);

    const es = new EventSource(`/api/intel/collect/progress/${collectId}/stream`);

    es.addEventListener('started', (e: MessageEvent) => {
      try {
        const d = JSON.parse(e.data);
        if (d.total_steps) setTotalSteps(d.total_steps);
      } catch { /* ignore */ }
    });

    es.addEventListener('step_start', (e: MessageEvent) => {
      try {
        const d = JSON.parse(e.data) as StepStartEvent;
        setTotalSteps(prev => d.total || prev);
        setSteps(prev => {
          const existingIdx = prev.findIndex(s => s.data_type === d.data_type);
          if (existingIdx >= 0) {
            const next = [...prev];
            next[existingIdx] = { ...next[existingIdx], status: 'running', display_name: d.display_name };
            return next;
          }
          return [...prev, {
            data_type: d.data_type,
            display_name: d.display_name,
            status: 'running' as const,
            records_fetched: 0,
            duration_ms: 0,
            error: null,
          }];
        });
      } catch { /* ignore malformed data */ }
    });

    es.addEventListener('step_complete', (e: MessageEvent) => {
      try {
        const d = JSON.parse(e.data) as StepCompleteEvent;
        setSteps(prev => {
          const next = [...prev];
          const step: ProgressStep = {
            data_type: d.data_type,
            status: d.status === 'success' ? 'success' : 'failed',
            source: d.source,
            records_fetched: d.records_fetched,
            duration_ms: d.duration_ms,
            error: d.error,
          };
          const idx = next.findIndex(s => s.data_type === d.data_type);
          if (idx >= 0) {
            next[idx] = step;
          } else {
            next.push(step);
          }
          return next;
        });
      } catch { /* ignore malformed data */ }
    });

    es.addEventListener('done', (e: MessageEvent) => {
      try {
        const d = JSON.parse(e.data) as CollectDoneEvent;
        setDoneEvent(d);
      } catch { /* ignore */ }
      setPhase('completed');
      es.close();
    });

    // Backend emits 'error' event type (not 'error_sse')
    es.addEventListener('error', (e: MessageEvent) => {
      // Distinguish SSE custom 'error' event from EventSource built-in error
      if (!(e instanceof MessageEvent) || typeof e.data !== 'string') return;
      try {
        const d = JSON.parse(e.data);
        setError(d.error || 'Unknown error');
      } catch {
        setError('Connection lost');
      }
      setPhase('failed');
      es.close();
    });

    // EventSource built-in error handler for connection-level errors
    es.onerror = () => {
      if (es.readyState === EventSource.CLOSED) {
        setError(prev => prev || 'Connection closed');
        setPhase(prev => prev === 'running' ? 'failed' : prev);
      }
    };

    return () => {
      es.close();
    };
  }, [collectId]);

  const currentStep = steps.filter(s => s.status !== 'pending').length;
  const effectiveTotalSteps = totalSteps || steps.length;

  return { steps, currentStep, totalSteps: effectiveTotalSteps, phase, doneEvent, error };
}
