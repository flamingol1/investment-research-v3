/**
 * 三情景对比图 - 乐观/中性/悲观目标价对比
 */
import React from 'react';
import ReactECharts from 'echarts-for-react';
import { useAppStore } from '../../lib/store';

interface ScenarioResult {
  scenario: string;
  target_price: number | null;
  upside_pct: number | null;
  probability: number | null;
}

interface Props {
  scenarios: ScenarioResult[];
  currentPrice: number | null;
}

const scenarioConfig: Record<string, { color: string; label: string }> = {
  '乐观': { color: '#ef4444', label: '乐观' },
  '中性': { color: '#3b82f6', label: '中性' },
  '悲观': { color: '#22c55e', label: '悲观' },
};

const ScenarioCompare: React.FC<Props> = ({ scenarios, currentPrice }) => {
  const theme = useAppStore((s) => s.theme);
  const isDark = theme === 'dark';

  const labels = scenarios.map((s) => s.scenario);
  const prices = scenarios.map((s) => s.target_price ?? 0);
  const upsides = scenarios.map((s) => s.upside_pct ?? 0);
  const colors = scenarios.map((s) => scenarioConfig[s.scenario]?.color ?? '#888');

  const option = {
    tooltip: {
      trigger: 'axis',
      backgroundColor: isDark ? '#242b3d' : '#fff',
      borderColor: isDark ? '#2e3a50' : '#e5e7eb',
      textStyle: { color: isDark ? '#e8ecf1' : '#1f2937' },
    },
    legend: {
      data: ['目标价', '上行空间%'],
      textStyle: { color: isDark ? '#8899aa' : '#6b7280' },
      top: 0,
    },
    grid: { left: 60, right: 60, top: 40, bottom: 30 },
    xAxis: {
      type: 'category',
      data: labels,
      axisLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb' } },
      axisLabel: { color: isDark ? '#8899aa' : '#6b7280' },
    },
    yAxis: [
      {
        type: 'value',
        name: '价格',
        axisLine: { show: false },
        splitLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb' } },
        axisLabel: { color: isDark ? '#8899aa' : '#6b7280' },
      },
      {
        type: 'value',
        name: '空间%',
        axisLine: { show: false },
        splitLine: { show: false },
        axisLabel: { color: isDark ? '#8899aa' : '#6b7280', formatter: '{value}%' },
      },
    ],
    series: [
      {
        name: '目标价',
        type: 'bar',
        data: prices.map((p, i) => ({
          value: p,
          itemStyle: { color: colors[i], borderRadius: [4, 4, 0, 0] },
        })),
        barWidth: 40,
        markLine: currentPrice
          ? {
              data: [{ yAxis: currentPrice, name: '当前价' }],
              lineStyle: { color: '#f59e0b', type: 'dashed' },
              label: { color: '#f59e0b', formatter: `当前价 ¥${currentPrice}` },
            }
          : undefined,
      },
      {
        name: '上行空间%',
        type: 'line',
        yAxisIndex: 1,
        data: upsides,
        lineStyle: { color: '#3b82f6', width: 2 },
        itemStyle: { color: '#3b82f6' },
        symbol: 'circle',
        symbolSize: 8,
      },
    ],
  };

  return <ReactECharts option={option} style={{ height: 280 }} opts={{ renderer: 'svg' }} />;
};

export default ScenarioCompare;
