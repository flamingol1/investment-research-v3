/**
 * 财务5维雷达图
 */
import React from 'react';
import ReactECharts from 'echarts-for-react';
import { useAppStore } from '../../lib/store';

interface DimensionScore {
  dimension: string;
  score: number;
  trend: string;
}

interface Props {
  dimensions: DimensionScore[];
}

const FinancialRadar: React.FC<Props> = ({ dimensions }) => {
  const theme = useAppStore((s) => s.theme);
  const isDark = theme === 'dark';

  const indicators = dimensions.map((d) => ({
    name: d.dimension,
    max: 10,
  }));

  const option = {
    tooltip: {
      trigger: 'item',
      backgroundColor: isDark ? '#242b3d' : '#fff',
      borderColor: isDark ? '#2e3a50' : '#e5e7eb',
      textStyle: { color: isDark ? '#e8ecf1' : '#1f2937' },
    },
    radar: {
      indicator: indicators,
      shape: 'polygon',
      splitNumber: 4,
      axisName: {
        color: isDark ? '#8899aa' : '#6b7280',
        fontSize: 12,
      },
      splitLine: {
        lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb' },
      },
      splitArea: {
        show: true,
        areaStyle: {
          color: isDark
            ? ['rgba(59,130,246,0.02)', 'rgba(59,130,246,0.05)']
            : ['rgba(59,130,246,0.02)', 'rgba(59,130,246,0.04)'],
        },
      },
      axisLine: {
        lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb' },
      },
    },
    series: [
      {
        type: 'radar',
        data: [
          {
            value: dimensions.map((d) => d.score),
            name: '当前评分',
            areaStyle: { color: 'rgba(59, 130, 246, 0.25)' },
            lineStyle: { color: '#3b82f6', width: 2 },
            itemStyle: { color: '#3b82f6' },
          },
        ],
      },
    ],
  };

  return <ReactECharts option={option} style={{ height: 300 }} opts={{ renderer: 'svg' }} />;
};

export default FinancialRadar;
