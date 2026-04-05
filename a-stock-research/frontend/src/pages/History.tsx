/**
 * 研究历史页面 - 按股票分组展示历史
 */
import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Card, Input, Tag, Typography, Empty, Spin, Button } from 'antd';
import {
  SearchOutlined, ArrowRightOutlined, HistoryOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { listReports, type ReportSummary } from '../lib/api';

const { Title, Text } = Typography;

const recColor: Record<string, string> = {
  '买入(强烈)': 'red',
  '买入(谨慎)': 'volcano',
  '持有': 'orange',
  '观望': 'blue',
  '卖出': 'green',
};

const riskColor: Record<string, string> = {
  '低': 'green',
  '中': 'orange',
  '高': 'red',
  '极高': 'red',
};

const HistoryPage: React.FC = () => {
  const navigate = useNavigate();
  const [filter, setFilter] = useState('');

  const { data: reports = [], isLoading } = useQuery({
    queryKey: ['reports'],
    queryFn: listReports,
  });

  // 按股票代码分组
  const grouped = reports.reduce<Record<string, ReportSummary[]>>((acc, r) => {
    const key = r.stock_code;
    if (!acc[key]) acc[key] = [];
    acc[key].push(r);
    return acc;
  }, {});

  // 过滤
  const filtered = Object.entries(grouped).filter(([code, entries]) => {
    if (!filter) return true;
    const q = filter.toLowerCase();
    return (
      code.toLowerCase().includes(q) ||
      entries[0]?.stock_name?.toLowerCase().includes(q)
    );
  });

  return (
    <div className="max-w-6xl mx-auto space-y-4">
      <div className="flex items-center justify-between">
        <Title level={4} style={{ color: 'var(--color-text-primary)', margin: 0 }}>
          <HistoryOutlined className="mr-2" />
          研究历史
        </Title>
        <Input
          placeholder="搜索股票代码或名称"
          prefix={<SearchOutlined />}
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          allowClear
          style={{ width: 260 }}
        />
      </div>

      {isLoading ? (
        <div className="flex justify-center py-20">
          <Spin size="large" />
        </div>
      ) : filtered.length === 0 ? (
        <Card>
          <Empty description={filter ? '未找到匹配结果' : '暂无研究历史，开始第一次研究吧'}>
            {!filter && (
              <Button type="primary" onClick={() => navigate('/research')}>
                开始研究
              </Button>
            )}
          </Empty>
        </Card>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {filtered.map(([code, entries]) => (
            <Card
              key={code}
              className="hover:border-blue-500/50 transition-colors cursor-pointer"
              style={{ background: 'var(--color-bg-secondary)' }}
              onClick={() => navigate(`/report/${code}/${entries[0].report_date}`)}
            >
              <div className="flex items-start justify-between">
                <div>
                  <div className="flex items-center gap-2 mb-1">
                    <Text strong style={{ color: 'var(--color-text-primary)', fontSize: 18, fontFamily: 'monospace' }}>
                      {code}
                    </Text>
                    <Text style={{ color: 'var(--color-text-secondary)' }}>
                      {entries[0]?.stock_name}
                    </Text>
                  </div>
                  <Text style={{ color: 'var(--color-text-muted)', fontSize: 12 }}>
                    共 {entries.length} 次研究 · 最近 {entries[0]?.report_date}
                  </Text>
                </div>
                <ArrowRightOutlined style={{ color: 'var(--color-text-muted)' }} />
              </div>

              {/* 最近结论摘要 */}
              {entries[0] && (
                <div className="mt-3 flex items-center gap-2 flex-wrap">
                  {entries[0].recommendation && (
                    <Tag color={recColor[entries[0].recommendation] || 'default'}>
                      {entries[0].recommendation}
                    </Tag>
                  )}
                  {entries[0].risk_level && (
                    <Tag color={riskColor[entries[0].risk_level] || 'default'}>
                      风险: {entries[0].risk_level}
                    </Tag>
                  )}
                  {entries[0].upside_pct != null && (
                    <Text
                      style={{
                        color: entries[0].upside_pct >= 0 ? 'var(--color-rise)' : 'var(--color-fall)',
                        fontSize: 13,
                      }}
                    >
                      {entries[0].upside_pct >= 0 ? '+' : ''}{entries[0].upside_pct.toFixed(1)}%
                    </Text>
                  )}
                </div>
              )}
            </Card>
          ))}
        </div>
      )}
    </div>
  );
};

export default HistoryPage;
