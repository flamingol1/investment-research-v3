/**
 * 图表包渲染器 - 根据后端 chart_pack 数据动态渲染各类图表
 */
import React from 'react';
import ReactECharts from 'echarts-for-react';
import { useAppStore } from '../../lib/store';
import type { ChartPackItem } from '../../lib/api';

interface Props {
  charts: ChartPackItem[];
}

const chartTypeMap: Record<string, string> = {
  line: '折线图',
  bar: '柱状图',
  table: '数据表',
  list: '指标列表',
};

function renderEmptyState(isDark: boolean, message = '暂无可展示数据') {
  return (
    <div className="h-[240px] flex items-center justify-center text-sm" style={{ color: isDark ? '#5c6b7f' : '#94a3b8' }}>
      {message}
    </div>
  );
}

function toFiniteNumber(value: unknown): number | null {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
}

function normalizeLabel(value: string | number | undefined) {
  if (value == null) {
    return '';
  }
  return String(value).trim();
}

function buildCartesianDataset(chart: ChartPackItem) {
  const categories: string[] = [];
  const seen = new Set<string>();
  const normalizedSeries = chart.series
    .map((series) => {
      const pointMap = new Map<string, number>();
      for (const point of series.points || []) {
        const label = normalizeLabel(point.x);
        const value = toFiniteNumber(point.y);
        if (!label || value == null) {
          continue;
        }
        if (!seen.has(label)) {
          seen.add(label);
          categories.push(label);
        }
        pointMap.set(label, value);
      }
      return {
        name: series.name,
        pointMap,
      };
    })
    .filter((series) => series.pointMap.size > 0);

  return {
    categories,
    series: normalizedSeries.map((series) => ({
      name: series.name,
      data: categories.map((label) => series.pointMap.get(label) ?? null),
    })),
  };
}

function renderLineChart(chart: ChartPackItem, isDark: boolean) {
  const colors = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#8b5cf6'];
  const dataset = buildCartesianDataset(chart);

  if (!dataset.categories.length || !dataset.series.length) {
    return renderEmptyState(isDark);
  }

  const series = dataset.series.map((s, i) => ({
    name: s.name,
    type: 'line',
    data: s.data,
    smooth: true,
    symbol: 'circle',
    symbolSize: 6,
    lineStyle: { width: 2.5, color: colors[i % colors.length] },
    itemStyle: { color: colors[i % colors.length] },
    areaStyle: {
      color: {
        type: 'linear',
        x: 0, y: 0, x2: 0, y2: 1,
        colorStops: [
          { offset: 0, color: `${colors[i % colors.length]}33` },
          { offset: 1, color: `${colors[i % colors.length]}05` },
        ],
      },
    },
    connectNulls: false,
  }));

  const option = {
    tooltip: {
      trigger: 'axis',
      backgroundColor: isDark ? '#242b3d' : '#fff',
      borderColor: isDark ? '#2e3a50' : '#e5e7eb',
      textStyle: { color: isDark ? '#e8ecf1' : '#1f2937' },
    },
    legend: {
      data: chart.series.map((s) => s.name),
      textStyle: { color: isDark ? '#8899aa' : '#6b7280', fontSize: 11 },
      top: 0,
      itemWidth: 14,
      itemHeight: 8,
    },
    grid: { left: 50, right: 20, top: 36, bottom: 28 },
    xAxis: {
      type: 'category',
      data: dataset.categories,
      axisLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb' } },
      axisLabel: { color: isDark ? '#8899aa' : '#6b7280', fontSize: 10 },
    },
    yAxis: {
      type: 'value',
      name: chart.unit,
      nameTextStyle: { color: isDark ? '#5c6b7f' : '#94a3b8', fontSize: 10 },
      axisLine: { show: false },
      splitLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb', type: 'dashed' } },
      axisLabel: { color: isDark ? '#8899aa' : '#6b7280', fontSize: 10 },
    },
    series,
  };

  return <ReactECharts option={option} style={{ height: 260 }} opts={{ renderer: 'svg' }} />;
}

function renderBarChart(chart: ChartPackItem, isDark: boolean) {
  const colors = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444'];
  const dataset = buildCartesianDataset(chart);

  if (!dataset.categories.length || !dataset.series.length) {
    return renderEmptyState(isDark);
  }

  const series = dataset.series.map((s, i) => ({
    name: s.name,
    type: 'bar',
    data: s.data,
    itemStyle: {
      color: colors[i % colors.length],
      borderRadius: [4, 4, 0, 0],
    },
    barMaxWidth: 28,
  }));

  const option = {
    tooltip: {
      trigger: 'axis',
      backgroundColor: isDark ? '#242b3d' : '#fff',
      borderColor: isDark ? '#2e3a50' : '#e5e7eb',
      textStyle: { color: isDark ? '#e8ecf1' : '#1f2937' },
    },
    legend: {
      data: chart.series.map((s) => s.name),
      textStyle: { color: isDark ? '#8899aa' : '#6b7280', fontSize: 11 },
      top: 0,
      itemWidth: 14,
      itemHeight: 8,
    },
    grid: { left: 50, right: 20, top: 36, bottom: 28 },
    xAxis: {
      type: 'category',
      data: dataset.categories,
      axisLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb' } },
      axisLabel: { color: isDark ? '#8899aa' : '#6b7280', fontSize: 10 },
    },
    yAxis: {
      type: 'value',
      name: chart.unit,
      nameTextStyle: { color: isDark ? '#5c6b7f' : '#94a3b8', fontSize: 10 },
      axisLine: { show: false },
      splitLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb', type: 'dashed' } },
      axisLabel: { color: isDark ? '#8899aa' : '#6b7280', fontSize: 10 },
    },
    series,
  };

  return <ReactECharts option={option} style={{ height: 260 }} opts={{ renderer: 'svg' }} />;
}

function renderPercentileChart(chart: ChartPackItem, isDark: boolean) {
  const series = chart.series[0];
  if (!series) return null;

  const points = series.points
    .map((point) => ({ label: normalizeLabel(point.x), value: toFiniteNumber(point.y) }))
    .filter((point): point is { label: string; value: number } => Boolean(point.label) && point.value != null);

  if (!points.length) {
    return renderEmptyState(isDark);
  }

  const values = points.map((p) => p.value);
  const labels = points.map((p) => p.label);

  const option = {
    tooltip: {
      trigger: 'axis',
      backgroundColor: isDark ? '#242b3d' : '#fff',
      borderColor: isDark ? '#2e3a50' : '#e5e7eb',
      textStyle: { color: isDark ? '#e8ecf1' : '#1f2937' },
      formatter: (params: unknown[]) => {
        const p = params[0] as { name: string; value: number };
        const val = Number(p.value).toFixed(1);
        const level = p.value >= 80 ? '偏高' : p.value >= 50 ? '中等' : '偏低';
        return `${p.name}: ${val}% (${level})`;
      },
    },
    grid: { left: 50, right: 30, top: 20, bottom: 40 },
    xAxis: {
      type: 'category',
      data: labels,
      axisLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb' } },
      axisLabel: { color: isDark ? '#8899aa' : '#6b7280', fontSize: 10 },
    },
    yAxis: {
      type: 'value',
      max: 100,
      axisLine: { show: false },
      splitLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb', type: 'dashed' } },
      axisLabel: { color: isDark ? '#8899aa' : '#6b7280', fontSize: 10, formatter: '{value}%' },
    },
    series: [
      {
        type: 'bar',
        data: values.map((v) => ({
          value: v,
          itemStyle: {
            color: v >= 80 ? '#ef4444' : v >= 60 ? '#f97316' : v >= 40 ? '#3b82f6' : '#22c55e',
            borderRadius: [4, 4, 0, 0],
          },
        })),
        barWidth: 50,
        label: {
          show: true,
          position: 'top',
          formatter: '{c}%',
          color: isDark ? '#e8ecf1' : '#1f2937',
          fontSize: 11,
          fontWeight: 'bold',
        },
      },
    ],
  };

  return <ReactECharts option={option} style={{ height: 240 }} opts={{ renderer: 'svg' }} />;
}

function renderScenarioChart(chart: ChartPackItem, isDark: boolean) {
  const scenarioColors: Record<string, string> = {
    '乐观': '#ef4444',
    '中性': '#3b82f6',
    '悲观': '#22c55e',
  };

  const points =
    chart.series.length === 1 && chart.series[0]?.points.length > 1
      ? chart.series[0].points
          .map((point) => ({
            label: normalizeLabel(point.x).replace('情景', ''),
            value: toFiniteNumber(point.y),
          }))
          .filter((point): point is { label: string; value: number } => Boolean(point.label) && point.value != null)
      : chart.series
          .map((series) => ({
            label: normalizeLabel(series.name).replace('情景', ''),
            value: toFiniteNumber(series.points[0]?.y),
          }))
          .filter((point): point is { label: string; value: number } => Boolean(point.label) && point.value != null);

  if (!points.length) {
    return renderEmptyState(isDark);
  }

  const labels = points.map((point) => point.label);
  const prices = points.map((point) => point.value);

  const option = {
    tooltip: {
      trigger: 'axis',
      backgroundColor: isDark ? '#242b3d' : '#fff',
      borderColor: isDark ? '#2e3a50' : '#e5e7eb',
      textStyle: { color: isDark ? '#e8ecf1' : '#1f2937' },
    },
    grid: { left: 50, right: 30, top: 20, bottom: 30 },
    xAxis: {
      type: 'category',
      data: labels,
      axisLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb' } },
      axisLabel: { color: isDark ? '#8899aa' : '#6b7280', fontSize: 11 },
    },
    yAxis: {
      type: 'value',
      name: chart.unit || '价格',
      nameTextStyle: { color: isDark ? '#5c6b7f' : '#94a3b8', fontSize: 10 },
      axisLine: { show: false },
      splitLine: { lineStyle: { color: isDark ? '#2e3a50' : '#e5e7eb', type: 'dashed' } },
      axisLabel: { color: isDark ? '#8899aa' : '#6b7280', fontSize: 10 },
    },
    series: [
      {
        type: 'bar',
        data: prices.map((p, i) => ({
          value: p,
          itemStyle: {
            color: scenarioColors[labels[i]] || '#3b82f6',
            borderRadius: [6, 6, 0, 0],
          },
        })),
        barWidth: 50,
        label: {
          show: true,
          position: 'top',
          formatter: `¥{c}`,
          color: isDark ? '#e8ecf1' : '#1f2937',
          fontSize: 12,
          fontWeight: 'bold',
        },
      },
    ],
  };

  return <ReactECharts option={option} style={{ height: 240 }} opts={{ renderer: 'svg' }} />;
}

function renderTableChart(chart: ChartPackItem, isDark: boolean) {
  const series = chart.series.filter((item) => item.points?.length);
  if (!series.length) {
    return renderEmptyState(isDark, '暂无数据');
  }

  const headers = series.map((s) => s.name);
  const rowCount = series[0]?.points.length || 0;
  const borderColor = isDark ? '#2e3a50' : '#e5e7eb';
  const rowBorderColor = isDark ? '#1e2330' : '#f1f5f9';

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr>
            <th
              className="text-left py-2 px-3 font-medium"
              style={{ color: isDark ? '#8899aa' : '#6b7280', borderBottom: '1px solid ' + borderColor }}
            >
              指标
            </th>
            {headers.map((h) => (
              <th
                key={h}
                className="text-right py-2 px-3 font-medium"
                style={{ color: isDark ? '#8899aa' : '#6b7280', borderBottom: '1px solid ' + borderColor }}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: rowCount }).map((_, rowIdx) => (
            <tr key={rowIdx} style={{ borderBottom: '1px solid ' + rowBorderColor }}>
              <td className="py-2 px-3 font-medium" style={{ color: isDark ? '#e8ecf1' : '#1f2937' }}>
                {series[0]?.points[rowIdx]?.x}
              </td>
              {series.map((s, colIdx) => (
                <td
                  key={colIdx}
                  className="text-right py-2 px-3"
                  style={{ color: isDark ? '#8899aa' : '#475569' }}
                >
                  {typeof s.points[rowIdx]?.y === 'number'
                    ? s.points[rowIdx]?.y.toFixed(2)
                    : s.points[rowIdx]?.y}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function renderListChart(chart: ChartPackItem, isDark: boolean) {
  const items = chart.series.flatMap((s) =>
    s.points
      .map((p) => ({ name: normalizeLabel(p.x), value: p.y, unit: chart.unit }))
      .filter((item) => item.name && item.value != null),
  );

  if (!items.length) {
    return renderEmptyState(isDark);
  }

  return (
    <div className="space-y-2">
      {items.map((item, i) => (
        <div key={i} className="flex items-center justify-between py-2 px-3 rounded-lg" style={{ background: isDark ? 'rgba(255,255,255,0.03)' : 'rgba(0,0,0,0.02)' }}>
          <span className="text-sm font-medium" style={{ color: isDark ? '#e8ecf1' : '#1f2937' }}>{item.name}</span>
          <span className="text-sm font-bold" style={{ color: isDark ? '#8899aa' : '#475569' }}>
            {typeof item.value === 'number' ? item.value.toFixed(1) : item.value}
            {item.unit ? ` ${item.unit}` : ''}
          </span>
        </div>
      ))}
    </div>
  );
}

function ChartCard({ chart, children }: { chart: ChartPackItem; children: React.ReactNode }) {
  const isDark = useAppStore((s) => s.theme) === 'dark';

  return (
    <div
      className="rounded-xl overflow-hidden"
      style={{
        background: 'var(--color-bg-secondary)',
        border: '1px solid var(--color-border)',
      }}
    >
      <div className="px-4 pt-4 pb-2 flex items-center justify-between">
        <div>
          <h3 className="text-sm font-semibold" style={{ color: 'var(--color-text-primary)' }}>
            {chart.title}
          </h3>
          {chart.summary && (
            <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-muted)' }}>
              {chart.summary}
            </p>
          )}
        </div>
        <span
          className="text-xs px-2 py-0.5 rounded-full"
          style={{
            background: isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.04)',
            color: 'var(--color-text-muted)',
          }}
        >
          {chartTypeMap[chart.chart_type] || chart.chart_type}
        </span>
      </div>
      <div className="px-2 pb-2">{children}</div>
    </div>
  );
}

const ChartPackRenderer: React.FC<Props> = ({ charts }) => {
  const theme = useAppStore((s) => s.theme);
  const isDark = theme === 'dark';

  if (!charts.length) {
    return (
      <div className="text-center py-12 rounded-xl" style={{ background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)' }}>
        <p className="text-sm" style={{ color: 'var(--color-text-muted)' }}>暂无图表数据</p>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
      {charts.map((chart) => {
        let content: React.ReactNode;

        switch (chart.chart_type) {
          case 'line':
            content = renderLineChart(chart, isDark);
            break;
          case 'bar':
            if (chart.chart_id === 'scenario_analysis') {
              content = renderScenarioChart(chart, isDark);
            } else {
              content = renderBarChart(chart, isDark);
            }
            break;
          case 'percentile':
            content = renderPercentileChart(chart, isDark);
            break;
          case 'table':
            content = renderTableChart(chart, isDark);
            break;
          case 'list':
            content = renderListChart(chart, isDark);
            break;
          default:
            content = renderLineChart(chart, isDark);
        }

        return (
          <ChartCard key={chart.chart_id} chart={chart}>
            {content}
          </ChartCard>
        );
      })}
    </div>
  );
};

export default ChartPackRenderer;
