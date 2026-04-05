/**
 * 报告详情页 - 完整研究报告展示 + 投资结论卡片
 */
import React from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Card, Tag, Button, Spin, Empty, Typography, Divider, Space } from 'antd';
import {
  ArrowLeftOutlined,
  PrinterOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import ReactMarkdown from 'react-markdown';
import rehypeRaw from 'rehype-raw';
import { getReport, type InvestmentConclusion } from '../lib/api';

const { Title, Text } = Typography;

const recommendationConfig: Record<string, { color: string; bg: string }> = {
  '买入(强烈)': { color: '#ef4444', bg: 'rgba(239, 68, 68, 0.1)' },
  '买入(谨慎)': { color: '#f97316', bg: 'rgba(249, 115, 22, 0.1)' },
  '持有': { color: '#f59e0b', bg: 'rgba(245, 158, 11, 0.1)' },
  '观望': { color: '#3b82f6', bg: 'rgba(59, 130, 246, 0.1)' },
  '卖出': { color: '#22c55e', bg: 'rgba(34, 197, 94, 0.1)' },
};

const riskConfig: Record<string, { color: string; bg: string }> = {
  '低': { color: '#22c55e', bg: 'rgba(34, 197, 94, 0.1)' },
  '中': { color: '#f59e0b', bg: 'rgba(245, 158, 11, 0.1)' },
  '高': { color: '#ef4444', bg: 'rgba(239, 68, 68, 0.1)' },
  '极高': { color: '#dc2626', bg: 'rgba(220, 38, 38, 0.15)' },
};

const ConclusionCard: React.FC<{ conclusion: InvestmentConclusion }> = ({ conclusion }) => {
  const recCfg = recommendationConfig[conclusion.recommendation] || { color: '#888', bg: 'rgba(136,136,136,0.1)' };
  const riskCfg = riskConfig[conclusion.risk_level] || { color: '#888', bg: 'rgba(136,136,136,0.1)' };

  return (
    <div
      className="rounded-xl p-5 space-y-4"
      style={{ background: 'var(--color-bg-elevated)', border: '1px solid var(--color-border)' }}
    >
      <div className="text-center">
        <div
          className="inline-block px-6 py-2 rounded-full text-xl font-bold"
          style={{ color: recCfg.color, background: recCfg.bg }}
        >
          {conclusion.recommendation}
        </div>
        <div className="mt-2 flex items-center justify-center gap-3">
          <Tag style={{ color: riskCfg.color, background: riskCfg.bg, border: 'none' }}>
            风险: {conclusion.risk_level}
          </Tag>
          <Tag style={{ color: '#3b82f6', background: 'rgba(59,130,246,0.1)', border: 'none' }}>
            置信度: {conclusion.confidence_level}
          </Tag>
        </div>
      </div>

      {/* 目标价 */}
      {conclusion.target_price_low != null && conclusion.target_price_high != null && (
        <div className="text-center">
          <Text style={{ color: 'var(--color-text-secondary)', fontSize: 12 }}>目标价区间</Text>
          <div className="text-2xl font-bold" style={{ color: 'var(--color-text-primary)' }}>
            ¥{conclusion.target_price_low.toFixed(2)} - ¥{conclusion.target_price_high.toFixed(2)}
          </div>
          {conclusion.current_price != null && conclusion.upside_pct != null && (
            <div
              className="text-lg font-semibold"
              style={{ color: conclusion.upside_pct >= 0 ? 'var(--color-rise)' : 'var(--color-fall)' }}
            >
              {conclusion.upside_pct >= 0 ? '+' : ''}{conclusion.upside_pct.toFixed(1)}%
              <span className="text-xs ml-1" style={{ color: 'var(--color-text-muted)' }}>vs 现价 ¥{conclusion.current_price.toFixed(2)}</span>
            </div>
          )}
        </div>
      )}

      <Divider style={{ borderColor: 'var(--color-border)', margin: '8px 0' }} />

      {/* 关键理由 */}
      {conclusion.key_reasons_buy?.length > 0 && (
        <div>
          <Text style={{ color: 'var(--color-rise)', fontSize: 12 }} className="block mb-1">买入理由</Text>
          <ul className="space-y-1 pl-4">
            {conclusion.key_reasons_buy.map((r, i) => (
              <li key={i} className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{r}</li>
            ))}
          </ul>
        </div>
      )}

      {conclusion.key_reasons_sell?.length > 0 && (
        <div>
          <Text style={{ color: 'var(--color-fall)', fontSize: 12 }} className="block mb-1">风险/卖出理由</Text>
          <ul className="space-y-1 pl-4">
            {conclusion.key_reasons_sell.map((r, i) => (
              <li key={i} className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{r}</li>
            ))}
          </ul>
        </div>
      )}

      {/* 仓位建议 */}
      {(conclusion.position_advice || conclusion.holding_period) && (
        <div className="grid grid-cols-2 gap-3">
          {conclusion.position_advice && (
            <div className="px-3 py-2 rounded-lg" style={{ background: 'var(--color-bg-secondary)' }}>
              <Text style={{ color: 'var(--color-text-muted)', fontSize: 11 }} className="block">仓位建议</Text>
              <Text style={{ color: 'var(--color-text-primary)', fontSize: 13 }}>{conclusion.position_advice}</Text>
            </div>
          )}
          {conclusion.holding_period && (
            <div className="px-3 py-2 rounded-lg" style={{ background: 'var(--color-bg-secondary)' }}>
              <Text style={{ color: 'var(--color-text-muted)', fontSize: 11 }} className="block">持有周期</Text>
              <Text style={{ color: 'var(--color-text-primary)', fontSize: 13 }}>{conclusion.holding_period}</Text>
            </div>
          )}
        </div>
      )}

      {/* 止损价 */}
      {conclusion.stop_loss_price != null && (
        <div className="text-center px-3 py-2 rounded-lg" style={{ background: 'rgba(239, 68, 68, 0.05)' }}>
          <Text style={{ color: 'var(--color-text-muted)', fontSize: 11 }} className="block">止损价</Text>
          <Text style={{ color: 'var(--color-danger)', fontSize: 16, fontWeight: 600 }}>
            ¥{conclusion.stop_loss_price.toFixed(2)}
          </Text>
        </div>
      )}

      {/* 结论摘要 */}
      {conclusion.conclusion_summary && (
        <div className="px-3 py-2 rounded-lg text-sm" style={{ background: 'var(--color-bg-secondary)', color: 'var(--color-text-secondary)' }}>
          {conclusion.conclusion_summary}
        </div>
      )}
    </div>
  );
};

const ReportDetailPage: React.FC = () => {
  const { stockCode, date } = useParams<{ stockCode: string; date: string }>();
  const navigate = useNavigate();

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

  return (
    <div className="max-w-7xl mx-auto">
      {/* 顶部操作栏 */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <Button
            type="text"
            icon={<ArrowLeftOutlined />}
            onClick={() => navigate(-1)}
            style={{ color: 'var(--color-text-secondary)' }}
          />
          <Title level={4} style={{ color: 'var(--color-text-primary)', margin: 0 }}>
            {report.stock_name || stockCode} 研究报告
          </Title>
          <Text style={{ color: 'var(--color-text-muted)' }}>{report.report_date}</Text>
        </div>
        <Space>
          <Button icon={<PrinterOutlined />} onClick={() => window.print()}>
            打印
          </Button>
        </Space>
      </div>

      <div className="flex gap-6">
        {/* 左侧：报告正文 */}
        <div className="flex-1 min-w-0">
          {report.markdown ? (
            <Card>
              <div className="markdown-content">
                <ReactMarkdown rehypePlugins={[rehypeRaw]}>
                  {report.markdown}
                </ReactMarkdown>
              </div>
            </Card>
          ) : (
            <Card>
              <Empty description="无Markdown报告内容" />
            </Card>
          )}
        </div>

        {/* 右侧：投资结论卡片 */}
        <div className="w-80 flex-shrink-0">
          <div className="sticky top-6">
            {report.conclusion ? (
              <ConclusionCard conclusion={report.conclusion} />
            ) : (
              <Card>
                <Empty description="无投资结论" />
              </Card>
            )}

            {/* Agent完成状态 */}
            {report.agents_completed?.length > 0 && (
              <div className="mt-4">
                <Text style={{ color: 'var(--color-text-muted)', fontSize: 12 }} className="block mb-2">
                  已完成分析模块
                </Text>
                <div className="flex flex-wrap gap-1">
                  {report.agents_completed.map((a) => (
                    <Tag key={a} style={{ background: 'var(--color-bg-elevated)', border: 'none', color: 'var(--color-text-secondary)' }}>
                      {a}
                    </Tag>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default ReportDetailPage;
