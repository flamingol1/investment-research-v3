/**
 * 风险6维雷达图
 */
import React from 'react';
import ReactECharts from 'echarts-for-react';
import { useAppStore } from '../../lib/store';

interface RiskItem {
  category: string;
  severity: string;
}

interface Props {
  risks: RiskItem[];
}

const severityToScore: Record<string, number> = {
  '高': 8,
  '中': 5,
  '低': 2,
};

const RiskRadar: React.FC<Props> = ({ risks }) => {
  const theme = useAppStore((s) => s.theme);
  const isDark = theme === 'dark';

  const categories = ['行业风险', '经营风险', '财务风险', '治理风险', '市场风险', '政策风险'];
  const scores = categories.map((cat) => {
    const found = risks.find((r) => r.category === cat);
    return found ? (severityToScore[found.severity] ?? 5) : 0;
  });

  const option = {
    tooltip: {
      backgroundColor: isDark ? '#242b3d' : '#fff',
      borderColor: isDark ? '#2e3a50' : '#e5e7eb',
      textStyle: { color: isDark ? '#e8ecf1' : '#1f2937' },
    },
    radar: {
      indicator: categories.map((name) => ({ name, max: 10 })),
      shape: 'polygon',
      splitNumber: 4,
      axisName: {
        color: isDark ? '#8899aa' : '#6b7280',
        fontSize: 12,
      },
      splitLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb' } },
      splitArea: {
        show: true,
        areaStyle: {
          color: isDark
            ? ['rgba(239,68,68,0.02)', 'rgba(239,68,68,0.05)']
            : ['rgba(239,68,68,0.02)', 'rgba(239,68,68,0.04)'],
        },
      },
      axisLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb' } },
    },
    series: [
      {
        type: 'radar',
        data: [
          {
            value: scores,
            name: '风险评分',
            areaStyle: { color: 'rgba(239, 68, 68, 0.2)' },
            lineStyle: { color: '#ef4444', width: 2 },
            itemStyle: { color: '#ef4444' },
          },
        ],
      },
    ],
  };

  return <ReactECharts option={option} style={{ height: 300 }} opts={{ renderer: 'svg' }} />;
};

export default RiskRadar;
