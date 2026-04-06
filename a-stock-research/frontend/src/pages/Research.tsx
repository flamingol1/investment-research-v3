/**
 * 股票研究页面
 */
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Button, Card, Progress, Select, Tag, Typography } from 'antd';
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import { useMutation } from '@tanstack/react-query';
import {
  connectResearchWS,
  getResearchStatus,
  startResearch,
  type ProgressDetail,
  type ProgressMessage,
  type ProgressMetric,
  type ResearchEvent,
  type ResearchStatus,
  type SecurityLookupItem,
} from '../lib/api';
import SecurityAutocomplete, {
  resolveTypedSecurityCode,
} from '../components/search/SecurityAutocomplete';

const { Title, Text } = Typography;

const AGENT_LABELS: Record<string, string> = {
  data_collector: '数据采集',
  data_cleaner: '数据清洗',
  screener: '初筛检查',
  financial: '财务分析',
  business_model: '商业模式',
  industry: '行业分析',
  governance: '治理分析',
  valuation: '估值分析',
  risk: '风险分析',
  report: '报告生成',
  conclusion: '投资结论',
  init: '初始化',
  done: '完成',
  error: '异常',
};

type TerminalTone = 'command' | 'running' | 'info' | 'success' | 'warning' | 'error';

interface TerminalLine {
  key: string;
  tone: TerminalTone;
  text: string;
}

const getAgentLabel = (agent: string) => AGENT_LABELS[agent] || agent || '处理中';

const formatEventTime = (value?: string) => {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return date.toLocaleTimeString('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
};

const toneColor = (tone: TerminalTone) => {
  switch (tone) {
    case 'command':
      return '#93c5fd';
    case 'running':
      return '#60a5fa';
    case 'success':
      return '#34d399';
    case 'warning':
      return '#fbbf24';
    case 'error':
      return '#f87171';
    default:
      return 'var(--color-text-secondary)';
  }
};

const linePrefix = (tone: TerminalTone) => {
  switch (tone) {
    case 'command':
      return '$';
    case 'success':
      return '+';
    case 'warning':
      return '!';
    case 'error':
      return 'x';
    default:
      return '>';
  }
};

const formatMetricLine = (metric: ProgressMetric) => `${metric.label}: ${metric.value}`;

const addEventLines = (lines: TerminalLine[], event: ResearchEvent) => {
  const timestamp = formatEventTime(event.created_at) || '--:--:--';
  const label = getAgentLabel(event.agent || event.stage);
  const title = event.detail?.headline || event.message || `${label} 已更新`;
  const tone: TerminalTone = event.status === 'failed'
    ? 'error'
    : event.status === 'completed'
      ? 'success'
      : 'running';

  lines.push({
    key: `${event.id}-headline`,
    tone,
    text: `[${timestamp}] [${label}] ${title}`,
  });

  const note = event.detail?.note?.trim();
  if (note && note !== title) {
    lines.push({
      key: `${event.id}-note`,
      tone: 'info',
      text: `[${timestamp}] ${note}`,
    });
  }

  event.detail?.metrics?.slice(0, 6).forEach((metric, index) => {
    lines.push({
      key: `${event.id}-metric-${metric.key}-${index}`,
      tone: metric.tone === 'danger'
        ? 'error'
        : metric.tone === 'warning'
          ? 'warning'
          : metric.tone === 'success'
            ? 'success'
            : 'info',
      text: `[${timestamp}] ${formatMetricLine(metric)}`,
    });
  });

  event.detail?.bullets?.slice(0, 6).forEach((bullet, index) => {
    lines.push({
      key: `${event.id}-bullet-${index}`,
      tone: 'info',
      text: `[${timestamp}] - ${bullet}`,
    });
  });
};

const Research: React.FC = () => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [stockCode, setStockCode] = useState(searchParams.get('stock') || '');
  const [selectedSecurity, setSelectedSecurity] = useState<SecurityLookupItem | null>(null);
  const [depth, setDepth] = useState<'quick' | 'standard' | 'deep'>('standard');
  const [taskId, setTaskId] = useState<string | null>(null);
  const [status, setStatus] = useState<ResearchStatus | null>(null);
  const [wsProgress, setWsProgress] = useState<ProgressMessage | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const terminalRef = useRef<HTMLDivElement | null>(null);

  const startMutation = useMutation({
    mutationFn: startResearch,
    onSuccess: (resp) => {
      const nextTaskId = resp.data?.task_id;
      if (!nextTaskId) return;

      wsRef.current?.close();
      setTaskId(nextTaskId);
      wsRef.current = connectResearchWS(nextTaskId, (msg) => {
        setWsProgress(msg);
        if (msg.status === 'completed' || msg.status === 'failed') {
          wsRef.current?.close();
        }
      });
      pollStatus(nextTaskId);
    },
  });

  const pollStatus = async (nextTaskId: string) => {
    const poll = async () => {
      try {
        const nextStatus = await getResearchStatus(nextTaskId);
        setStatus(nextStatus);
        if (nextStatus.status !== 'completed' && nextStatus.status !== 'failed') {
          setTimeout(poll, 3000);
        }
      } catch {
        setTimeout(poll, 5000);
      }
    };

    poll();
  };

  const resolvedStockCode = selectedSecurity?.stock_code || resolveTypedSecurityCode(stockCode);

  const handleStart = (nextSecurity?: SecurityLookupItem) => {
    const normalizedCode = nextSecurity?.stock_code
      || selectedSecurity?.stock_code
      || resolveTypedSecurityCode(stockCode);
    if (!normalizedCode) return;

    wsRef.current?.close();
    setStatus(null);
    setWsProgress(null);
    startMutation.mutate({ stock_code: normalizedCode, depth });
  };

  useEffect(() => () => {
    wsRef.current?.close();
  }, []);

  const hasNavigableReport = !!(
    status?.report
    && (
      status.report.has_full_report
      || status.report.recommendation
      || status.report.risk_level
      || status.report.agents_completed?.length
    )
  );

  useEffect(() => {
    if (status?.status === 'completed' && status.report && hasNavigableReport) {
      const timer = setTimeout(() => {
        navigate(`/report/${status.stock_code}/${status.report!.report_date}`);
      }, 2000);
      return () => clearTimeout(timer);
    }
    return undefined;
  }, [hasNavigableReport, navigate, status]);

  const currentProgress = wsProgress?.progress ?? status?.progress ?? 0;
  const taskStatus = wsProgress?.status ?? status?.status ?? '';
  const currentMessage = wsProgress?.message ?? status?.message ?? '';
  const stageDetail: ProgressDetail | null = wsProgress?.stage_detail ?? status?.stage_detail ?? null;
  const recentEvents = wsProgress?.recent_events ?? status?.recent_events ?? [];
  const visibleErrors = status?.errors ?? [];

  const mergedEvents = useMemo(() => {
    const eventMap = new Map<number, ResearchEvent>();

    recentEvents.forEach((event) => {
      eventMap.set(event.id, event);
    });

    if (wsProgress?.event) {
      eventMap.set(wsProgress.event.id, wsProgress.event);
    }

    return Array.from(eventMap.values()).sort((left, right) => left.id - right.id);
  }, [recentEvents, wsProgress?.event]);

  const terminalLines = useMemo(() => {
    const lines: TerminalLine[] = [];
    const command = ['$ research'];

    if (resolvedStockCode) {
      command.push(resolvedStockCode);
    }
    command.push(`--depth ${depth}`);

    lines.push({
      key: 'command',
      tone: 'command',
      text: command.join(' '),
    });

    if (taskId) {
      lines.push({
        key: 'task-id',
        tone: 'info',
        text: `task_id=${taskId}`,
      });
    }

    if (mergedEvents.length === 0) {
      lines.push({
        key: 'boot',
        tone: startMutation.isPending ? 'running' : 'info',
        text: startMutation.isPending ? '任务已提交，等待后端返回第一条进度...' : '准备启动研究任务...',
      });
    } else {
      mergedEvents.forEach((event) => addEventLines(lines, event));
    }

    const lastEvent = mergedEvents[mergedEvents.length - 1];
    const lastHeadline = lastEvent?.detail?.headline || lastEvent?.message || '';
    const liveTimestamp = formatEventTime(wsProgress?.timestamp) || formatEventTime(status?.started_at ?? undefined) || '--:--:--';
    const liveHeadline = stageDetail?.headline?.trim() || currentMessage.trim();
    const liveNote = stageDetail?.note?.trim() || '';

    if (
      taskStatus === 'running'
      && liveHeadline
      && liveHeadline !== lastHeadline
    ) {
      lines.push({
        key: 'live-headline',
        tone: 'running',
        text: `[${liveTimestamp}] [${getAgentLabel(wsProgress?.agent ?? status?.current_agent ?? '')}] ${liveHeadline}`,
      });

      if (liveNote && liveNote !== liveHeadline) {
        lines.push({
          key: 'live-note',
          tone: 'info',
          text: `[${liveTimestamp}] ${liveNote}`,
        });
      }

      stageDetail?.metrics?.slice(0, 6).forEach((metric, index) => {
        lines.push({
          key: `live-metric-${metric.key}-${index}`,
          tone: metric.tone === 'danger'
            ? 'error'
            : metric.tone === 'warning'
              ? 'warning'
              : metric.tone === 'success'
                ? 'success'
                : 'info',
          text: `[${liveTimestamp}] ${formatMetricLine(metric)}`,
        });
      });
    }

    if (taskStatus === 'completed') {
      lines.push({
        key: 'done',
        tone: 'success',
        text: '研究完成，正在跳转到报告页面...',
      });
    }

    if (taskStatus === 'failed') {
      lines.push({
        key: 'failed',
        tone: 'error',
        text: '研究失败，后端已停止当前任务。',
      });
    }

    visibleErrors.slice(0, 8).forEach((error, index) => {
      lines.push({
        key: `error-${index}`,
        tone: taskStatus === 'completed' ? 'warning' : 'error',
        text: error,
      });
    });

    return lines.slice(-200);
  }, [
    currentMessage,
    depth,
    mergedEvents,
    resolvedStockCode,
    stageDetail,
    startMutation.isPending,
    status?.current_agent,
    status?.started_at,
    taskId,
    taskStatus,
    visibleErrors,
    wsProgress?.agent,
    wsProgress?.timestamp,
  ]);

  useEffect(() => {
    const terminal = terminalRef.current;
    if (!terminal) return;
    terminal.scrollTop = terminal.scrollHeight;
  }, [terminalLines]);

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <Title level={3} style={{ color: 'var(--color-text-primary)' }}>
        股票深度研究
      </Title>

      <Card>
        <div className="flex gap-3 items-end flex-wrap md:flex-nowrap">
          <div className="flex-1 min-w-[220px]">
            <Text style={{ color: 'var(--color-text-secondary)' }} className="block mb-2">
              股票代码
            </Text>
            <SecurityAutocomplete
              value={stockCode}
              selectedSecurity={selectedSecurity}
              onValueChange={setStockCode}
              onSelectedSecurityChange={setSelectedSecurity}
              onSubmit={handleStart}
              placeholder="输入代码、名称或拼音，例如 600519 / 贵州茅台 / gzmt"
              maxWidth={360}
            />
          </div>
          <div>
            <Text style={{ color: 'var(--color-text-secondary)' }} className="block mb-2">
              研究深度
            </Text>
            <Select
              size="large"
              value={depth}
              onChange={setDepth}
              style={{ width: 140 }}
              options={[
                { value: 'quick', label: '快速' },
                { value: 'standard', label: '标准' },
                { value: 'deep', label: '深度' },
              ]}
            />
          </div>
          <Button
            type="primary"
            size="large"
            icon={<ThunderboltOutlined />}
            onClick={() => handleStart()}
            loading={startMutation.isPending}
            disabled={!resolvedStockCode}
          >
            开始研究
          </Button>
        </div>
      </Card>

      {(taskId || startMutation.isPending) && (
        <Card>
          <div className="flex items-center justify-between gap-4 flex-wrap mb-4">
            <div className="flex items-center gap-2 flex-wrap">
              <Text style={{ color: 'var(--color-text-primary)' }} strong>
                研究进度
              </Text>
              {resolvedStockCode && <Tag color="blue">{resolvedStockCode}</Tag>}
              {taskStatus === 'completed' && <Tag color="success">已完成</Tag>}
              {taskStatus === 'failed' && <Tag color="error">失败</Tag>}
            </div>
            <Text style={{ color: 'var(--color-text-secondary)' }}>
              {Math.round(currentProgress * 100)}%
            </Text>
          </div>

          <Progress
            percent={Math.round(currentProgress * 100)}
            status={taskStatus === 'failed' ? 'exception' : taskStatus === 'completed' ? 'success' : 'active'}
            strokeColor="#3b82f6"
          />

          <div
            className="mt-5 overflow-hidden rounded-2xl"
            style={{
              background: '#11161f',
              border: '1px solid rgba(148, 163, 184, 0.18)',
              boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.03)',
            }}
          >
            <div
              className="flex items-center justify-between gap-3 px-4 py-3"
              style={{
                background: 'linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.01))',
                borderBottom: '1px solid rgba(148, 163, 184, 0.14)',
              }}
            >
              <div className="flex items-center gap-3">
                <div className="flex items-center gap-2">
                  <span className="h-2.5 w-2.5 rounded-full bg-red-400 inline-block" />
                  <span className="h-2.5 w-2.5 rounded-full bg-amber-400 inline-block" />
                  <span className="h-2.5 w-2.5 rounded-full bg-emerald-400 inline-block" />
                </div>
                <Text
                  style={{
                    color: 'var(--color-text-primary)',
                    fontFamily: 'Cascadia Code, Consolas, ui-monospace, monospace',
                  }}
                >
                  研究控制台
                </Text>
              </div>

              <Text
                style={{
                  color: 'var(--color-text-muted)',
                  fontSize: 12,
                  fontFamily: 'Cascadia Code, Consolas, ui-monospace, monospace',
                }}
              >
                {taskId ? `task ${taskId.slice(0, 8)}` : 'waiting'}
              </Text>
            </div>

            <div
              ref={terminalRef}
              className="max-h-[560px] min-h-[320px] overflow-auto px-4 py-4"
              style={{
                fontFamily: 'Cascadia Code, Consolas, ui-monospace, monospace',
                backgroundImage: 'linear-gradient(rgba(148,163,184,0.035) 1px, transparent 1px)',
                backgroundSize: '100% 28px',
              }}
            >
              <div className="space-y-2">
                {terminalLines.map((line) => (
                  <div
                    key={line.key}
                    className="flex items-start gap-3 text-sm leading-7 whitespace-pre-wrap break-words"
                  >
                    <span
                      style={{
                        color: toneColor(line.tone),
                        width: 12,
                        flexShrink: 0,
                        textAlign: 'center',
                      }}
                    >
                      {linePrefix(line.tone)}
                    </span>
                    <span
                      style={{
                        color: line.tone === 'command'
                          ? '#dbeafe'
                          : line.tone === 'error'
                            ? '#fecaca'
                            : line.tone === 'warning'
                              ? '#fde68a'
                              : line.tone === 'success'
                                ? '#bbf7d0'
                                : 'var(--color-text-secondary)',
                      }}
                    >
                      {line.text}
                    </span>
                  </div>
                ))}
              </div>

              {taskStatus === 'running' && (
                <div className="flex items-center gap-3 text-sm leading-7 mt-2">
                  <span
                    className="progress-pulse"
                    style={{
                      color: '#60a5fa',
                      width: 12,
                      flexShrink: 0,
                      textAlign: 'center',
                    }}
                  >
                    _
                  </span>
                  <span style={{ color: 'var(--color-text-muted)' }}>
                    等待下一条进度输出...
                  </span>
                </div>
              )}
            </div>
          </div>

          {taskStatus === 'completed' && hasNavigableReport && (
            <div className="mt-4 flex items-center gap-2 text-green-400">
              <CheckCircleOutlined />
              <span>报告已生成，正在跳转...</span>
            </div>
          )}

          {taskStatus === 'failed' && (
            <div className="mt-4 flex items-center gap-2 text-red-400">
              <CloseCircleOutlined />
              <span>任务执行失败，请查看上面的控制台输出。</span>
            </div>
          )}
        </Card>
      )}
    </div>
  );
};

export default Research;
