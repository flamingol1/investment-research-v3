/**
 * 估值百分位图 - PE/PB历史分位面积图
 */
import React from 'react';
import ReactECharts from 'echarts-for-react';
import { useAppStore } from '../../lib/store';

interface Props {
  pePercentile: number | null;
  pbPercentile: number | null;
  peCurrent: number | null;
  pbCurrent: number | null;
}

const ValuationPercentile: React.FC<Props> = ({
  pePercentile, pbPercentile,
}) => {
  const theme = useAppStore((s) => s.theme);
  const isDark = theme === 'dark';

  const categories = ['PE(TTM)', 'PB(MRQ)'];
  const percentileValues = [pePercentile ?? 0, pbPercentile ?? 0];

  const option = {
    tooltip: {
      trigger: 'axis',
      backgroundColor: isDark ? '#242b3d' : '#fff',
      borderColor: isDark ? '#2e3a50' : '#e5e7eb',
      textStyle: { color: isDark ? '#e8ecf1' : '#1f2937' },
      formatter: (params: unknown[]) => {
        const p = params[0] as { name: string; value: number };
        const val = p.value.toFixed(1);
        const level = p.value >= 80 ? '偏高' : p.value >= 50 ? '中等' : '偏低';
        return `${p.name}: ${val}% (${level})`;
      },
    },
    grid: { left: 60, right: 30, top: 20, bottom: 40 },
    xAxis: {
      type: 'category',
      data: categories,
      axisLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb' } },
      axisLabel: { color: isDark ? '#8899aa' : '#6b7280' },
    },
    yAxis: {
      type: 'value',
      max: 100,
      axisLine: { show: false },
      splitLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb' } },
      axisLabel: { color: isDark ? '#8899aa' : '#6b7280', formatter: '{value}%' },
    },
    series: [
      {
        type: 'bar',
        data: percentileValues.map((v) => ({
          value: v,
          itemStyle: {
            color: v >= 80
              ? '#ef4444'
              : v >= 60
              ? '#f97316'
              : v >= 40
              ? '#3b82f6'
              : '#22c55e',
          },
        })),
        barWidth: 50,
        label: {
          show: true,
          position: 'top',
          formatter: '{c}%',
          color: isDark ? '#e8ecf1' : '#1f2937',
        },
      },
    ],
  };

  return <ReactECharts option={option} style={{ height: 240 }} opts={{ renderer: 'svg' }} />;
};

export default ValuationPercentile;
