/**
 * 报告详情页 - 精美研究报告展示
 * 包含：头部信息、核心指标、图表网格、Markdown正文、投资结论、证据包、Agent状态
 */
import React, { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Card, Tag, Button, Spin, Empty, Typography, Divider, Tabs, Badge } from 'antd';
import {
  ArrowLeftOutlined,
  PrinterOutlined,
  FileTextOutlined,
  BarChartOutlined,
  SafetyCertificateOutlined,
  RobotOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  RiseOutlined,
  FallOutlined,
  AimOutlined,
  ClockCircleOutlined,
  SafetyOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { getReport, type InvestmentConclusion, type ReportDetail } from '../lib/api';
import ConclusionGauge from '../components/charts/ConclusionGauge';
import ChartPackRenderer from '../components/charts/ChartPackRenderer';
import EvidencePack from '../components/EvidencePack';
import ReportMarkdown from '../components/report/ReportMarkdown';

const { Text } = Typography;

// ============ 配置映射 ============
const recommendationConfig: Record<string, { color: string; bg: string; icon: React.ReactNode }> = {
  '买入(强烈)': { color: '#ef4444', bg: 'rgba(239, 68, 68, 0.1)', icon: <RiseOutlined /> },
  '买入(谨慎)': { color: '#f97316', bg: 'rgba(249, 115, 22, 0.1)', icon: <RiseOutlined /> },
  '持有': { color: '#f59e0b', bg: 'rgba(245, 158, 11, 0.1)', icon: <AimOutlined /> },
  '观望': { color: '#3b82f6', bg: 'rgba(59, 130, 246, 0.1)', icon: <ClockCircleOutlined /> },
  '卖出': { color: '#22c55e', bg: 'rgba(34, 197, 94, 0.1)', icon: <FallOutlined /> },
};

const riskConfig: Record<string, { color: string; bg: string }> = {
  '低': { color: '#22c55e', bg: 'rgba(34, 197, 94, 0.1)' },
  '中': { color: '#f59e0b', bg: 'rgba(245, 158, 11, 0.1)' },
  '高': { color: '#ef4444', bg: 'rgba(239, 68, 68, 0.1)' },
  '极高': { color: '#dc2626', bg: 'rgba(220, 38, 38, 0.15)' },
};

const depthConfig: Record<string, { label: string; color: string }> = {
  quick: { label: '快速研究', color: '#3b82f6' },
  standard: { label: '标准深度', color: '#8b5cf6' },
  deep: { label: '深度研究', color: '#ef4444' },
};

// ============ 核心指标卡片 ============
const MetricCard: React.FC<{
  label: string;
  value: React.ReactNode;
  suffix?: string;
  highlight?: boolean;
  tone?: 'neutral' | 'positive' | 'negative' | 'warning';
}> = ({ label, value, suffix, highlight = false, tone = 'neutral' }) => {
  const toneColors = {
    neutral: { text: 'var(--color-text-primary)', bg: 'var(--color-bg-elevated)' },
    positive: { text: 'var(--color-rise)', bg: 'rgba(220, 38, 38, 0.06)' },
    negative: { text: 'var(--color-fall)', bg: 'rgba(22, 163, 74, 0.06)' },
    warning: { text: '#f59e0b', bg: 'rgba(245, 158, 11, 0.06)' },
  };
  const tc = toneColors[tone];

  return (
    <div
      className="rounded-xl px-5 py-4 flex flex-col justify-center"
      style={{
        background: highlight ? tc.bg : 'var(--color-bg-secondary)',
        border: '1px solid var(--color-border)',
        minHeight: 88,
      }}
    >
      <Text style={{ color: 'var(--color-text-muted)', fontSize: 12 }} className="mb-1">
        {label}
      </Text>
      <div className="flex items-baseline gap-1">
        <span
          className="text-xl font-bold tracking-tight"
          style={{ color: highlight ? tc.text : 'var(--color-text-primary)' }}
        >
          {value}
        </span>
        {suffix && (
          <span className="text-xs" style={{ color: 'var(--color-text-muted)' }}>
            {suffix}
          </span>
        )}
      </div>
    </div>
  );
};

// ============ 投资结论侧边栏卡片 ============
const ConclusionCard: React.FC<{ conclusion: InvestmentConclusion }> = ({ conclusion }) => {
  const recCfg = recommendationConfig[conclusion.recommendation] || {
    color: '#888',
    bg: 'rgba(136,136,136,0.1)',
    icon: null,
  };
  const riskCfg = riskConfig[conclusion.risk_level] || { color: '#888', bg: 'rgba(136,136,136,0.1)' };

  return (
    <div
      className="rounded-xl p-5 space-y-4"
      style={{ background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)' }}
    >
      {/* 评级 */}
      <div className="text-center pb-4" style={{ borderBottom: '1px solid var(--color-border)' }}>
        <div
          className="inline-flex items-center gap-2 px-5 py-2 rounded-full text-lg font-bold"
          style={{ color: recCfg.color, background: recCfg.bg }}
        >
          {recCfg.icon}
          {conclusion.recommendation}
        </div>
        <div className="mt-3 flex items-center justify-center gap-2">
          <Tag style={{ color: riskCfg.color, background: riskCfg.bg, border: 'none' }}>
            <SafetyOutlined className="mr-1" />
            风险: {conclusion.risk_level}
          </Tag>
          <Tag style={{ color: '#3b82f6', background: 'rgba(59,130,246,0.1)', border: 'none' }}>
            <CheckCircleOutlined className="mr-1" />
            置信度: {conclusion.confidence_level}
          </Tag>
        </div>
      </div>

      {/* 目标价 */}
      {conclusion.target_price_low != null && conclusion.target_price_high != null && (
        <div className="text-center py-2">
          <Text style={{ color: 'var(--color-text-muted)', fontSize: 12 }} className="block mb-1">
            目标价区间
          </Text>
          <div className="text-2xl font-bold" style={{ color: 'var(--color-text-primary)' }}>
            ¥{conclusion.target_price_low.toFixed(2)} - ¥{conclusion.target_price_high.toFixed(2)}
          </div>
          {conclusion.current_price != null && conclusion.upside_pct != null && (
            <div
              className="text-base font-semibold mt-1"
              style={{ color: conclusion.upside_pct >= 0 ? 'var(--color-rise)' : 'var(--color-fall)' }}
            >
              {conclusion.upside_pct >= 0 ? '+' : ''}{conclusion.upside_pct.toFixed(1)}%
              <span className="text-xs ml-1 font-normal" style={{ color: 'var(--color-text-muted)' }}>
                vs 现价 ¥{conclusion.current_price.toFixed(2)}
              </span>
            </div>
          )}
        </div>
      )}

      <Divider style={{ borderColor: 'var(--color-border)', margin: '8px 0' }} />

      {/* 买入理由 */}
      {conclusion.key_reasons_buy?.length > 0 && (
        <div>
          <Text style={{ color: 'var(--color-rise)', fontSize: 12 }} className="block mb-2 font-medium">
            <RiseOutlined className="mr-1" />
            买入理由
          </Text>
          <ul className="space-y-2 pl-0">
            {conclusion.key_reasons_buy.map((r, i) => (
              <li key={i} className="flex items-start gap-2 text-sm" style={{ color: 'var(--color-text-secondary)' }}>
                <span
                  className="w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 mt-0.5 text-xs font-bold"
                  style={{ background: 'rgba(220,38,38,0.1)', color: 'var(--color-rise)' }}
                >
                  {i + 1}
                </span>
                <span className="leading-relaxed">{r}</span>
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* 风险理由 */}
      {conclusion.key_reasons_sell?.length > 0 && (
        <div>
          <Text style={{ color: 'var(--color-fall)', fontSize: 12 }} className="block mb-2 font-medium">
            <FallOutlined className="mr-1" />
            风险/卖出理由
          </Text>
          <ul className="space-y-2 pl-0">
            {conclusion.key_reasons_sell.map((r, i) => (
              <li key={i} className="flex items-start gap-2 text-sm" style={{ color: 'var(--color-text-secondary)' }}>
                <span
                  className="w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 mt-0.5 text-xs font-bold"
                  style={{ background: 'rgba(22,163,74,0.1)', color: 'var(--color-fall)' }}
                >
                  {i + 1}
                </span>
                <span className="leading-relaxed">{r}</span>
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* 核心假设 */}
      {conclusion.key_assumptions?.length > 0 && (
        <div>
          <Text style={{ color: 'var(--color-text-muted)', fontSize: 12 }} className="block mb-2 font-medium">
            核心假设
          </Text>
          <div className="space-y-1.5">
            {conclusion.key_assumptions.map((a, i) => (
              <div
                key={i}
                className="text-sm px-3 py-2 rounded-lg"
                style={{ background: 'var(--color-bg-elevated)', color: 'var(--color-text-secondary)' }}
              >
                {a}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 跟踪指标 */}
      {conclusion.monitoring_points?.length > 0 && (
        <div>
          <Text style={{ color: 'var(--color-text-muted)', fontSize: 12 }} className="block mb-2 font-medium">
            需跟踪指标
          </Text>
          <div className="space-y-1.5">
            {conclusion.monitoring_points.map((p, i) => (
              <div
                key={i}
                className="text-sm px-3 py-2 rounded-lg flex items-center gap-2"
                style={{ background: 'var(--color-bg-elevated)', color: 'var(--color-text-secondary)' }}
              >
                <ClockCircleOutlined style={{ color: 'var(--color-brand)', fontSize: 12 }} />
                {p}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 仓位与周期 */}
      <div className="grid grid-cols-2 gap-3">
        {conclusion.position_advice && (
          <div
            className="px-3 py-3 rounded-lg text-center"
            style={{ background: 'var(--color-bg-elevated)' }}
          >
            <Text style={{ color: 'var(--color-text-muted)', fontSize: 11 }} className="block mb-1">
              仓位建议
            </Text>
            <Text style={{ color: 'var(--color-text-primary)', fontSize: 14, fontWeight: 600 }}>
              {conclusion.position_advice}
            </Text>
          </div>
        )}
        {conclusion.holding_period && (
          <div
            className="px-3 py-3 rounded-lg text-center"
            style={{ background: 'var(--color-bg-elevated)' }}
          >
            <Text style={{ color: 'var(--color-text-muted)', fontSize: 11 }} className="block mb-1">
              持有周期
            </Text>
            <Text style={{ color: 'var(--color-text-primary)', fontSize: 14, fontWeight: 600 }}>
              {conclusion.holding_period}
            </Text>
          </div>
        )}
      </div>

      {/* 止损价 */}
      {conclusion.stop_loss_price != null && (
        <div className="text-center px-4 py-3 rounded-lg" style={{ background: 'rgba(239, 68, 68, 0.05)' }}>
          <Text style={{ color: 'var(--color-text-muted)', fontSize: 11 }} className="block mb-1">
            止损价
          </Text>
          <Text style={{ color: 'var(--color-danger)', fontSize: 18, fontWeight: 700 }}>
            ¥{conclusion.stop_loss_price.toFixed(2)}
          </Text>
        </div>
      )}

      {/* 结论摘要 */}
      {conclusion.conclusion_summary && (
        <div
          className="px-4 py-3 rounded-lg text-sm leading-relaxed"
          style={{
            background: 'var(--color-bg-elevated)',
            color: 'var(--color-text-secondary)',
            borderLeft: `3px solid ${recCfg.color}`,
          }}
        >
          {conclusion.conclusion_summary}
        </div>
      )}
    </div>
  );
};

// ============ Agent 状态组件 ============
const AgentStatus: React.FC<{ agents: string[]; skipped: string[] }> = ({ agents, skipped }) => {
  const allAgents = [...agents, ...skipped];

  return (
    <div className="space-y-2">
      {agents.map((a) => (
        <div
          key={a}
          className="flex items-center gap-2 text-sm px-3 py-2 rounded-lg"
          style={{ background: 'rgba(34,197,94,0.06)' }}
        >
          <CheckCircleOutlined style={{ color: '#22c55e', fontSize: 14 }} />
          <span style={{ color: 'var(--color-text-secondary)' }}>{a}</span>
          <Badge status="success" text="完成" style={{ marginLeft: 'auto', fontSize: 11 }} />
        </div>
      ))}
      {skipped.map((a) => (
        <div
          key={a}
          className="flex items-center gap-2 text-sm px-3 py-2 rounded-lg"
          style={{ background: 'var(--color-bg-elevated)' }}
        >
          <ExclamationCircleOutlined style={{ color: 'var(--color-text-muted)', fontSize: 14 }} />
          <span style={{ color: 'var(--color-text-muted)' }}>{a}</span>
          <span className="text-xs ml-auto" style={{ color: 'var(--color-text-muted)' }}>
            跳过
          </span>
        </div>
      ))}
      {allAgents.length === 0 && (
        <Text style={{ color: 'var(--color-text-muted)', fontSize: 12 }}>暂无Agent执行记录</Text>
      )}
    </div>
  );
};

const ReportQualityBanner: React.FC<{ report: ReportDetail }> = ({ report }) => {
  const qualityGate = report.quality_gate;
  const severityColor = qualityGate?.blocked ? '#dc2626' : '#f59e0b';
  const severityBg = qualityGate?.blocked ? 'rgba(220, 38, 38, 0.08)' : 'rgba(245, 158, 11, 0.08)';
  const signalTitle = qualityGate?.blocked ? '当前版本为证据受限摘要' : '当前版本存在明显研究边界';
  const signalBody = qualityGate?.blocked
    ? '证据闸门未通过，页面内容更适合作为阶段性研究摘要，不宜直接当作完整投资报告。'
    : '部分模块或关键字段不完整，建议结合证据链和后续补数结果再做判断。';

  const signals = Array.from(
    new Set([
      ...(qualityGate?.reasons || []),
      ...(qualityGate?.consistency_notes || []),
      ...report.agents_skipped.map((agent) => `${agent} 模块本次未形成稳定结论`),
      ...report.errors,
    ]),
  ).slice(0, 6);

  const metrics = [
    {
      label: '核心证据分',
      value: qualityGate ? `${Math.round((qualityGate.core_evidence_score || 0) * 100)}%` : 'N/A',
    },
    {
      label: '覆盖率',
      value: qualityGate ? `${Math.round((qualityGate.coverage_ratio || 0) * 100)}%` : 'N/A',
    },
    {
      label: '跳过模块',
      value: String(report.agents_skipped.length),
    },
  ];

  return (
    <Card
      className="mb-6"
      style={{
        background: severityBg,
        borderColor: `${severityColor}55`,
      }}
    >
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-2 mb-2">
            <ExclamationCircleOutlined style={{ color: severityColor }} />
            <Text strong style={{ color: severityColor }}>
              {signalTitle}
            </Text>
          </div>
          <div className="text-sm leading-7" style={{ color: 'var(--color-text-secondary)' }}>
            {signalBody}
          </div>
          {signals.length > 0 && (
            <div className="mt-3 space-y-1.5">
              {signals.map((item, index) => (
                <div key={`${item}-${index}`} className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
                  {item}
                </div>
              ))}
            </div>
          )}
        </div>
        <div className="grid grid-cols-3 gap-3 min-w-[280px]">
          {metrics.map((metric) => (
            <div
              key={metric.label}
              className="rounded-xl px-4 py-3"
              style={{ background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)' }}
            >
              <div className="text-xs mb-1" style={{ color: 'var(--color-text-muted)' }}>
                {metric.label}
              </div>
              <div className="text-lg font-semibold" style={{ color: 'var(--color-text-primary)' }}>
                {metric.value}
              </div>
            </div>
          ))}
        </div>
      </div>
    </Card>
  );
};

// ============ 主页面 ============
const ReportDetailPage: React.FC = () => {
  const { stockCode, date } = useParams<{ stockCode: string; date: string }>();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState('charts');

  const { data: report, isLoading, error } = useQuery({
    queryKey: ['report', stockCode, date],
    queryFn: () => getReport(stockCode!, date!),
    enabled: !!stockCode && !!date,
  });

  if (isLoading) {
    return (
      <div className="flex justify-center items-center h-96">
        <Spin size="large" />
      </div>
    );
  }

  if (error || !report) {
    return (
      <div className="flex flex-col items-center justify-center h-96">
        <Empty description="报告不存在或加载失败" />
        <Button className="mt-4" onClick={() => navigate(-1)}>
          返回
        </Button>
      </div>
    );
  }

  const depth = depthConfig[report.depth] || { label: report.depth, color: '#888' };
  const conclusion = report.conclusion;
  const recCfg = conclusion ? recommendationConfig[conclusion.recommendation] : null;
  const qualityGate = report.quality_gate;
  const isEvidenceLimited = Boolean(
    qualityGate?.blocked || report.agents_skipped.length > 0 || report.errors.length > 0,
  );
  const reportTabLabel = isEvidenceLimited ? '研究摘要' : '研究报告';

  return (
    <div className="max-w-7xl mx-auto pb-12">
      {/* ==================== 报告头部 Hero ==================== */}
      <div
        className="rounded-2xl p-6 mb-6 relative overflow-hidden"
        style={{
          background: 'var(--color-bg-secondary)',
          border: '1px solid var(--color-border)',
        }}
      >
        {/* 背景装饰 */}
        <div
          className="absolute top-0 right-0 w-64 h-64 rounded-full opacity-5 pointer-events-none"
          style={{
            background: `radial-gradient(circle, ${recCfg?.color || '#3b82f6'} 0%, transparent 70%)`,
            transform: 'translate(30%, -30%)',
          }}
        />

        <div className="relative z-10">
          {/* 顶部操作栏 */}
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-3">
              <Button
                type="text"
                icon={<ArrowLeftOutlined />}
                onClick={() => navigate(-1)}
                style={{ color: 'var(--color-text-secondary)' }}
              />
              <span
                className="text-xs px-2 py-0.5 rounded-full font-medium"
                style={{ background: `${depth.color}15`, color: depth.color }}
              >
                {depth.label}
              </span>
            </div>
            <Button icon={<PrinterOutlined />} onClick={() => window.print()}>
              打印报告
            </Button>
          </div>

          {/* 股票信息 */}
          <div className="flex items-end gap-4 mb-4">
            <div>
              <div className="flex items-center gap-3">
                <h1 className="text-3xl font-bold" style={{ color: 'var(--color-text-primary)' }}>
                  {report.stock_name || stockCode}
                </h1>
                <span
                  className="text-lg font-mono px-3 py-1 rounded-lg"
                  style={{
                    background: 'var(--color-bg-elevated)',
                    color: 'var(--color-text-secondary)',
                  }}
                >
                  {report.stock_code}
                </span>
              </div>
              <p className="text-sm mt-1" style={{ color: 'var(--color-text-muted)' }}>
                报告日期: {report.report_date}
              </p>
            </div>

            {conclusion && (
              <div className="ml-auto flex items-center gap-4">
                <ConclusionGauge
                  recommendation={conclusion.recommendation}
                  upsidePct={conclusion.upside_pct}
                  confidenceLevel={conclusion.confidence_level}
                />
              </div>
            )}
          </div>

          {/* 核心指标行 */}
          {conclusion && (
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3 mt-4">
              {conclusion.target_price_low != null && conclusion.target_price_high != null && (
                <MetricCard
                  label="目标价区间"
                  value={`¥${conclusion.target_price_low.toFixed(2)}-${conclusion.target_price_high.toFixed(2)}`}
                  highlight
                  tone="neutral"
                />
              )}
              {conclusion.current_price != null && (
                <MetricCard
                  label="当前价格"
                  value={`¥${conclusion.current_price.toFixed(2)}`}
                />
              )}
              {conclusion.upside_pct != null && (
                <MetricCard
                  label="预期收益"
                  value={`${conclusion.upside_pct >= 0 ? '+' : ''}${conclusion.upside_pct.toFixed(1)}%`}
                  highlight
                  tone={conclusion.upside_pct >= 0 ? 'positive' : 'negative'}
                />
              )}
              <MetricCard
                label="风险等级"
                value={conclusion.risk_level}
                highlight
                tone={conclusion.risk_level === '高' || conclusion.risk_level === '极高' ? 'warning' : 'neutral'}
              />
              <MetricCard
                label="置信度"
                value={conclusion.confidence_level}
              />
              {conclusion.stop_loss_price != null && (
                <MetricCard
                  label="止损价"
                  value={`¥${conclusion.stop_loss_price.toFixed(2)}`}
                  highlight
                  tone="negative"
                />
              )}
            </div>
          )}
        </div>
      </div>

      {isEvidenceLimited && <ReportQualityBanner report={report} />}

      {/* ==================== 图表 + 正文标签页 ==================== */}
      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        items={[
          {
            key: 'charts',
            label: (
              <span className="flex items-center gap-1.5">
                <BarChartOutlined />
                数据图表 ({report.chart_pack.length})
              </span>
            ),
            children: (
              <div className="mt-4">
                <ChartPackRenderer charts={report.chart_pack} />
              </div>
            ),
          },
          {
            key: 'report',
            label: (
              <span className="flex items-center gap-1.5">
                <FileTextOutlined />
                {reportTabLabel}
              </span>
            ),
            children: (
              <div className="mt-4 flex gap-6">
                {/* 左侧：Markdown正文 */}
                <div className="flex-1 min-w-0">
                  {report.markdown ? (
                    <Card>
                      <ReportMarkdown markdown={report.markdown} charts={report.chart_pack} />
                    </Card>
                  ) : (
                    <Card>
                      <Empty description="无Markdown报告内容" />
                    </Card>
                  )}
                </div>

                {/* 右侧：投资结论侧边栏 */}
                <div className="w-80 flex-shrink-0 hidden xl:block">
                  <div className="sticky top-6 space-y-4">
                    {conclusion ? (
                      <ConclusionCard conclusion={conclusion} />
                    ) : (
                      <Card>
                        <Empty description="无投资结论" />
                      </Card>
                    )}
                  </div>
                </div>
              </div>
            ),
          },
          {
            key: 'evidence',
            label: (
              <span className="flex items-center gap-1.5">
                <SafetyCertificateOutlined />
                证据链 ({report.evidence_pack.length})
              </span>
            ),
            children: (
              <div className="mt-4">
                <EvidencePack evidences={report.evidence_pack} />
              </div>
            ),
          },
          {
            key: 'agents',
            label: (
              <span className="flex items-center gap-1.5">
                <RobotOutlined />
                分析模块 ({report.agents_completed.length})
              </span>
            ),
            children: (
              <div className="mt-4 max-w-2xl">
                <Card
                  title="Agent 执行状态"
                  style={{ background: 'var(--color-bg-secondary)', borderColor: 'var(--color-border)' }}
                >
                  <AgentStatus
                    agents={report.agents_completed}
                    skipped={report.agents_skipped}
                  />
                </Card>
              </div>
            ),
          },
        ]}
      />

      {/* 移动端投资结论（在正文标签页中未显示时） */}
      {activeTab !== 'report' && conclusion && (
        <div className="xl:hidden mt-6">
          <ConclusionCard conclusion={conclusion} />
        </div>
      )}
    </div>
  );
};

export default ReportDetailPage;
