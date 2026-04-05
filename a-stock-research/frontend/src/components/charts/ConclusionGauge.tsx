/**
 * 投资结论仪表盘 - 评级 + 置信度 + 上下行空间
 */
import React from 'react';
import ReactECharts from 'echarts-for-react';
import { useAppStore } from '../../lib/store';

interface Props {
  recommendation: string;
  upsidePct: number | null;
  confidenceLevel: string;
}

const recToScore: Record<string, number> = {
  '买入(强烈)': 90,
  '买入(谨慎)': 72,
  '持有': 55,
  '观望': 35,
  '卖出': 15,
};

const recToColor: Record<string, string> = {
  '买入(强烈)': '#ef4444',
  '买入(谨慎)': '#f97316',
  '持有': '#f59e0b',
  '观望': '#3b82f6',
  '卖出': '#22c55e',
};

const ConclusionGauge: React.FC<Props> = ({ recommendation, confidenceLevel }) => {
  const theme = useAppStore((s) => s.theme);
  const score = recToScore[recommendation] ?? 50;
  const color = recToColor[recommendation] ?? '#888';

  const option = {
    series: [
      {
        type: 'gauge',
        startAngle: 200,
        endAngle: -20,
        center: ['50%', '60%'],
        radius: '90%',
        min: 0,
        max: 100,
        splitNumber: 5,
        axisLine: {
          lineStyle: {
            width: 18,
            color: [
              [0.3, '#22c55e'],
              [0.5, '#f59e0b'],
              [0.7, '#f97316'],
              [1, '#ef4444'],
            ],
          },
        },
        pointer: {
          icon: 'path://M12.8,0.7l12,40.1H0.7L12.8,0.7z',
          length: '55%',
          width: 10,
          offsetCenter: [0, '-10%'],
          itemStyle: { color: color },
        },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { show: false },
        title: {
          show: true,
          offsetCenter: [0, '25%'],
          fontSize: 16,
          fontWeight: 'bold',
          color: theme === 'dark' ? '#e8ecf1' : '#1f2937',
        },
        detail: {
          valueAnimation: true,
          offsetCenter: [0, '55%'],
          fontSize: 28,
          fontWeight: 'bold',
          color: color,
          formatter: `{value|${recommendation}}`,
          rich: {
            value: { fontSize: 20, fontWeight: 'bold', color: color },
          },
        },
        data: [{ value: score, name: confidenceLevel || '' }],
      },
    ],
  };

  return (
    <ReactECharts
      option={option}
      style={{ height: 220 }}
      opts={{ renderer: 'svg' }}
      theme={theme === 'dark' ? 'dark' : undefined}
    />
  );
};

export default ConclusionGauge;
