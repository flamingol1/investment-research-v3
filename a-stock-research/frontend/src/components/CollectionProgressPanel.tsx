/**
 * 采集进度面板 — 实时展示每个数据类型的采集状态
 */
import {
  Progress,
  List,
  Tag,
  Typography,
  Space,
} from 'antd';
import {
  LoadingOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';
import type { ProgressStep, ProgressPhase } from '../lib/useCollectionProgress';
import type { CollectDoneEvent } from '../lib/api';

const { Text } = Typography;

const DATA_TYPE_LABELS: Record<string, string> = {
  stock_info: '股票基础信息',
  daily_prices: '历史日线行情',
  realtime_quote: '实时行情',
  financials: '财务报表',
  valuation: '估值数据',
  announcements: '公告披露',
  governance: '公司治理',
  research_reports: '研报摘要',
  shareholders: '股东数据',
  industry: '行业数据',
  valuation_pct: '估值分位',
  news: '新闻资讯',
};

const STATUS_COLORS = {
  success: '#22c55e',
  error: '#ef4444',
  running: '#3b82f6',
  muted: '#6b7280',
} as const;

interface CollectionProgressPanelProps {
  steps: ProgressStep[];
  currentStep: number;
  totalSteps: number;
  phase: ProgressPhase;
  doneEvent: CollectDoneEvent | null;
  error: string | null;
  stockCode: string;
}

function StepIcon({ status }: { status: ProgressStep['status'] }) {
  switch (status) {
    case 'running':
      return <LoadingOutlined style={{ color: STATUS_COLORS.running }} />;
    case 'success':
      return <CheckCircleOutlined style={{ color: STATUS_COLORS.success }} />;
    case 'failed':
      return <CloseCircleOutlined style={{ color: STATUS_COLORS.error }} />;
    default:
      return <ClockCircleOutlined style={{ color: STATUS_COLORS.muted }} />;
  }
}

function CollectionProgressPanel({
  steps,
  currentStep,
  totalSteps,
  phase,
  doneEvent,
  error,
  stockCode,
}: CollectionProgressPanelProps) {
  const progressPercent = totalSteps > 0 ? Math.round((currentStep / totalSteps) * 100) : 0;
  const runningStep = steps.find(s => s.status === 'running');

  // 状态文本
  let statusText = `准备采集 ${stockCode} 的数据...`;
  if (phase === 'running' && runningStep) {
    const label = DATA_TYPE_LABELS[runningStep.data_type] || runningStep.display_name || runningStep.data_type;
    statusText = `正在采集: ${label} (${currentStep}/${totalSteps})`;
  } else if (phase === 'completed') {
    statusText = `采集完成`;
  } else if (phase === 'failed') {
    statusText = `采集失败`;
  }

  return (
    <div className="space-y-3">
      {/* 进度条 */}
      <div className="space-y-1">
        <div className="flex items-center justify-between">
          <Text style={{ color: 'var(--color-text-primary)', fontSize: 13 }}>
            {statusText}
          </Text>
          <Text style={{ color: 'var(--color-text-muted)', fontSize: 12 }}>
            {currentStep}/{totalSteps}
          </Text>
        </div>
        <Progress
          percent={progressPercent}
          size="small"
          status={
            phase === 'failed' ? 'exception' :
            phase === 'completed' ? 'success' :
            'active'
          }
          strokeColor={
            phase === 'completed' ? STATUS_COLORS.success :
            phase === 'failed' ? STATUS_COLORS.error :
            STATUS_COLORS.running
          }
        />
      </div>

      {/* 步骤列表 */}
      <List
        size="small"
        dataSource={steps}
        renderItem={(step) => {
          const label = DATA_TYPE_LABELS[step.data_type] || step.display_name || step.data_type;
          return (
            <List.Item style={{ padding: '4px 0' }}>
              <div className="flex items-center justify-between w-full">
                <Space size="small">
                  <StepIcon status={step.status} />
                  <Text style={{
                    color: step.status === 'running' ? STATUS_COLORS.running :
                           step.status === 'success' ? STATUS_COLORS.success :
                           step.status === 'failed' ? STATUS_COLORS.error :
                           'var(--color-text-secondary)',
                    fontSize: 13,
                  }}>
                    {label}
                  </Text>
                  {step.source && (
                    <Text style={{ color: 'var(--color-text-muted)', fontSize: 11 }}>
                      via {step.source}
                    </Text>
                  )}
                </Space>
                <Space size="small">
                  {step.status === 'success' && step.records_fetched > 0 && (
                    <Text style={{ color: 'var(--color-text-muted)', fontSize: 12 }}>
                      {step.records_fetched}条
                    </Text>
                  )}
                  {step.duration_ms > 0 && (
                    <Text style={{ color: 'var(--color-text-muted)', fontSize: 11 }}>
                      {step.duration_ms}ms
                    </Text>
                  )}
                  {step.status === 'success' && (
                    <Tag color="success" style={{ fontSize: 11, lineHeight: '18px' }}>成功</Tag>
                  )}
                  {step.status === 'failed' && (
                    <Tag color="error" style={{ fontSize: 11, lineHeight: '18px' }}>失败</Tag>
                  )}
                  {step.status === 'running' && (
                    <Tag color="processing" style={{ fontSize: 11, lineHeight: '18px' }}>采集中</Tag>
                  )}
                </Space>
              </div>
              {step.error && (
                <Text style={{ color: STATUS_COLORS.error, fontSize: 12 }} className="block mt-1">
                  {step.error}
                </Text>
              )}
            </List.Item>
          );
        }}
      />

      {/* 完成汇总 */}
      {phase === 'completed' && doneEvent && (
        <div
          className="flex items-center justify-center gap-4 py-2 rounded"
          style={{ background: 'var(--color-bg-secondary)' }}
        >
          <Text style={{ color: STATUS_COLORS.success, fontSize: 14 }}>
            {doneEvent.success_count} 成功
          </Text>
          {doneEvent.failed_count > 0 && (
            <Text style={{ color: STATUS_COLORS.error, fontSize: 14 }}>
              {doneEvent.failed_count} 失败
            </Text>
          )}
        </div>
      )}

      {/* 错误信息 */}
      {phase === 'failed' && error && (
        <Text style={{ color: STATUS_COLORS.error, fontSize: 13 }} className="block">
          {error}
        </Text>
      )}
    </div>
  );
}

export default CollectionProgressPanel;
