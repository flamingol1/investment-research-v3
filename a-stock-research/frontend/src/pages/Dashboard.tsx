/**
 * 仪表盘页面 - 概览、最近研究、监控摘要
 */
import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Card, Statistic, Tag, Empty, Spin, Typography } from 'antd';
import {
  EyeOutlined,
  FileSearchOutlined,
  AlertOutlined,
  RightOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { listReports, getWatchList } from '../lib/api';

const { Title, Text } = Typography;

const Dashboard: React.FC = () => {
  const navigate = useNavigate();

  const { data: reports = [], isLoading: reportsLoading } = useQuery({
    queryKey: ['reports'],
    queryFn: listReports,
  });

  const { data: watchData } = useQuery({
    queryKey: ['watch'],
    queryFn: getWatchList,
  });

  const watchItems = watchData?.items ?? [];
  const alertCount = watchItems.filter((i) => i.status === 'warning' || i.status === 'critical').length;

  const recommendationColor: Record<string, string> = {
    '买入(强烈)': 'red',
    '买入(谨慎)': 'volcano',
    '持有': 'orange',
    '观望': 'blue',
    '卖出': 'green',
  };

  return (
    <div className="space-y-6 max-w-7xl">
      {/* 统计卡片 */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card
          className="cursor-pointer hover:border-blue-500/50 transition-colors"
          onClick={() => navigate('/watch')}
        >
          <Statistic
            title={<Text style={{ color: 'var(--color-text-secondary)' }}>监控标的</Text>}
            value={watchItems.length}
            prefix={<EyeOutlined style={{ color: '#3b82f6' }} />}
            suffix={
              alertCount > 0 && (
                <Tag color="red" className="ml-2">
                  {alertCount} 预警
                </Tag>
              )
            }
            valueStyle={{ color: 'var(--color-text-primary)' }}
          />
        </Card>

        <Card
          className="cursor-pointer hover:border-blue-500/50 transition-colors"
          onClick={() => navigate('/history')}
        >
          <Statistic
            title={<Text style={{ color: 'var(--color-text-secondary)' }}>研究报告</Text>}
            value={reports.length}
            prefix={<FileSearchOutlined style={{ color: '#10b981' }} />}
            valueStyle={{ color: 'var(--color-text-primary)' }}
          />
        </Card>

        <Card>
          <Statistic
            title={<Text style={{ color: 'var(--color-text-secondary)' }}>系统状态</Text>}
            value="运行中"
            prefix={<AlertOutlined style={{ color: '#22c55e' }} />}
            valueStyle={{ color: '#22c55e', fontSize: 20 }}
          />
        </Card>
      </div>

      {/* 快速研究入口 */}
      <Card
        className="cursor-pointer hover:border-blue-500/50 transition-colors"
        style={{
          background: 'linear-gradient(135deg, var(--color-bg-secondary), var(--color-bg-elevated))',
          border: '1px solid var(--color-border)',
        }}
        onClick={() => navigate('/research')}
      >
        <div className="flex items-center justify-between py-4">
          <div>
            <Title level={4} style={{ color: 'var(--color-text-primary)', margin: 0 }}>
              开始新的股票研究
            </Title>
            <Text style={{ color: 'var(--color-text-secondary)' }}>
              输入股票代码，AI自动完成7阶段深度研究分析
            </Text>
          </div>
          <div className="flex items-center gap-2 text-blue-400">
            <span>开始研究</span>
            <RightOutlined />
          </div>
        </div>
      </Card>

      {/* 最近研究报告 */}
      <div>
        <div className="flex items-center justify-between mb-4">
          <Title level={5} style={{ color: 'var(--color-text-primary)', margin: 0 }}>
            最近研究报告
          </Title>
          <button
            className="text-sm text-blue-400 hover:text-blue-300"
            onClick={() => navigate('/history')}
          >
            查看全部 <RightOutlined />
          </button>
        </div>

        {reportsLoading ? (
          <div className="flex justify-center py-12">
            <Spin />
          </div>
        ) : reports.length === 0 ? (
          <Card>
            <Empty description="暂无研究报告，开始第一次研究吧" />
          </Card>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {reports.slice(0, 6).map((r) => (
              <Card
                key={`${r.stock_code}-${r.report_date}`}
                className="cursor-pointer hover:border-blue-500/50 transition-colors"
                onClick={() => navigate(`/report/${r.stock_code}/${r.report_date}`)}
                size="small"
              >
                <div className="flex items-start justify-between">
                  <div>
                    <div className="flex items-center gap-2 mb-1">
                      <Text strong style={{ color: 'var(--color-text-primary)', fontSize: 16 }}>
                        {r.stock_code}
                      </Text>
                      {r.stock_name && (
                        <Text style={{ color: 'var(--color-text-secondary)' }}>
                          {r.stock_name}
                        </Text>
                      )}
                    </div>
                    <Text style={{ color: 'var(--color-text-muted)', fontSize: 12 }}>
                      {r.report_date}
                    </Text>
                  </div>
                  {r.recommendation && (
                    <Tag color={recommendationColor[r.recommendation] || 'default'}>
                      {r.recommendation}
                    </Tag>
                  )}
                </div>
                {r.current_price && (
                  <div className="mt-3 flex items-center gap-4">
                    <Text style={{ color: 'var(--color-text-secondary)', fontSize: 13 }}>
                      当前价: ¥{r.current_price.toFixed(2)}
                    </Text>
                    {r.upside_pct != null && (
                      <Text
                        style={{
                          color: r.upside_pct >= 0 ? 'var(--color-rise)' : 'var(--color-fall)',
                          fontSize: 13,
                        }}
                      >
                        {r.upside_pct >= 0 ? '+' : ''}
                        {r.upside_pct.toFixed(1)}%
                      </Text>
                    )}
                  </div>
                )}
              </Card>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default Dashboard;
