/**
 * 股票研究页面 - 发起研究 + 实时进度 + 结果展示
 */
import React, { useState, useEffect, useRef } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Card, Input, Select, Button, Tag, Typography, Progress } from 'antd';
import {
  SearchOutlined,
  ThunderboltOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  LoadingOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';
import { useMutation } from '@tanstack/react-query';
import { startResearch, getResearchStatus, connectResearchWS, type ResearchStatus, type ProgressMessage } from '../lib/api';

const { Title, Text } = Typography;

const AGENT_LABELS: Record<string, string> = {
  collector: '数据采集',
  cleaner: '数据清洗',
  screener: '初筛检查',
  financial: '财务分析',
  business_model: '商业模式',
  industry: '行业分析',
  governance: '治理分析',
  valuation: '估值分析',
  risk: '风险分析',
  report: '报告生成',
  conclusion: '投资结论',
};

const Research: React.FC = () => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [stockCode, setStockCode] = useState(searchParams.get('stock') || '');
  const [depth, setDepth] = useState<'quick' | 'standard' | 'deep'>('standard');
  const [taskId, setTaskId] = useState<string | null>(null);
  const [status, setStatus] = useState<ResearchStatus | null>(null);
  const [wsProgress, setWsProgress] = useState<ProgressMessage | null>(null);
  const wsRef = useRef<WebSocket | null>(null);

  const startMutation = useMutation({
    mutationFn: startResearch,
    onSuccess: (resp) => {
      const tid = resp.data?.task_id;
      if (tid) {
        setTaskId(tid);
        // 连接WebSocket
        wsRef.current = connectResearchWS(tid, (msg) => {
          setWsProgress(msg);
          if (msg.status === 'completed' || msg.status === 'failed') {
            wsRef.current?.close();
          }
        });
        // 同时轮询作为后备
        pollStatus(tid);
      }
    },
  });

  const pollStatus = async (tid: string) => {
    const poll = async () => {
      try {
        const s = await getResearchStatus(tid);
        setStatus(s);
        if (s.status !== 'completed' && s.status !== 'failed') {
          setTimeout(poll, 3000);
        }
      } catch {
        setTimeout(poll, 5000);
      }
    };
    poll();
  };

  const handleStart = () => {
    if (!stockCode.trim()) return;
    startMutation.mutate({ stock_code: stockCode.trim(), depth });
  };

  // 清理WebSocket
  useEffect(() => {
    return () => {
      wsRef.current?.close();
    };
  }, []);

  // 当研究完成时跳转报告页
  useEffect(() => {
    if (status?.status === 'completed' && status.report) {
      const timer = setTimeout(() => {
        navigate(`/report/${status.stock_code}/${status.report!.report_date}`);
      }, 2000);
      return () => clearTimeout(timer);
    }
  }, [status?.status]);

  const currentProgress = wsProgress?.progress ?? status?.progress ?? 0;
  const currentAgent = wsProgress?.agent ?? status?.current_agent ?? '';
  const taskStatus = wsProgress?.status ?? status?.status ?? '';

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <Title level={3} style={{ color: 'var(--color-text-primary)' }}>
        股票深度研究
      </Title>

      {/* 输入区 */}
      <Card>
        <div className="flex gap-3 items-end">
          <div className="flex-1">
            <Text style={{ color: 'var(--color-text-secondary)' }} className="block mb-2">
              股票代码
            </Text>
            <Input
              size="large"
              placeholder="输入6位股票代码，如 300358"
              value={stockCode}
              onChange={(e) => setStockCode(e.target.value)}
              onPressEnter={handleStart}
              prefix={<SearchOutlined />}
              style={{ maxWidth: 300 }}
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
            onClick={handleStart}
            loading={startMutation.isPending}
            disabled={!stockCode.trim()}
          >
            开始研究
          </Button>
        </div>
      </Card>

      {/* 进度面板 */}
      {(taskId || startMutation.isPending) && (
        <Card>
          <div className="mb-4">
            <div className="flex items-center justify-between mb-2">
              <Text style={{ color: 'var(--color-text-primary)' }} strong>
                研究进度
                {stockCode && (
                  <Tag className="ml-2" color="blue">{stockCode}</Tag>
                )}
              </Text>
              <Text style={{ color: 'var(--color-text-secondary)' }}>
                {Math.round(currentProgress * 100)}%
              </Text>
            </div>
            <Progress
              percent={Math.round(currentProgress * 100)}
              status={taskStatus === 'failed' ? 'exception' : 'active'}
              strokeColor="#3b82f6"
            />
          </div>

          {/* Agent网格 */}
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mt-4">
            {Object.entries(AGENT_LABELS).map(([key, label]) => {
              const isActive = currentAgent.includes(key);
              const isDone = (currentProgress > 0.5 && !isActive) ||
                (status?.report && status.report.agents_completed?.includes(key));

              return (
                <div
                  key={key}
                  className="flex items-center gap-2 px-3 py-2 rounded-lg text-sm"
                  style={{
                    background: isActive ? 'rgba(59, 130, 246, 0.15)' : 'var(--color-bg-elevated)',
                    border: isActive ? '1px solid rgba(59, 130, 246, 0.3)' : '1px solid transparent',
                  }}
                >
                  {isActive ? (
                    <LoadingOutlined className="text-blue-400 progress-pulse" />
                  ) : isDone ? (
                    <CheckCircleOutlined className="text-green-400" />
                  ) : (
                    <ClockCircleOutlined style={{ color: 'var(--color-text-muted)' }} />
                  )}
                  <span
                    style={{
                      color: isActive
                        ? '#60a5fa'
                        : isDone
                        ? 'var(--color-text-primary)'
                        : 'var(--color-text-muted)',
                    }}
                  >
                    {label}
                  </span>
                </div>
              );
            })}
          </div>

          {/* 当前状态消息 */}
          {wsProgress?.message && (
            <div
              className="mt-4 px-3 py-2 rounded-lg text-sm"
              style={{ background: 'var(--color-bg-elevated)', color: 'var(--color-text-secondary)' }}
            >
              {wsProgress.message}
            </div>
          )}

          {/* 完成提示 */}
          {status?.status === 'completed' && (
            <div className="mt-4 p-4 rounded-lg text-center" style={{ background: 'rgba(16, 185, 129, 0.1)' }}>
              <CheckCircleOutlined className="text-2xl text-green-400 mb-2" />
              <div className="text-green-400">研究完成，正在跳转到报告页面...</div>
            </div>
          )}

          {/* 错误提示 */}
          {status?.status === 'failed' && (
            <div className="mt-4 p-4 rounded-lg" style={{ background: 'rgba(239, 68, 68, 0.1)' }}>
              <CloseCircleOutlined className="text-xl text-red-400" />
              <div className="text-red-400 mt-1">研究失败</div>
              {status.errors.map((e, i) => (
                <div key={i} className="text-red-300 text-sm mt-1">{e}</div>
              ))}
            </div>
          )}
        </Card>
      )}
    </div>
  );
};

export default Research;
