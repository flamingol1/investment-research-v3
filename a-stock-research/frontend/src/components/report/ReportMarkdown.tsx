import React from 'react';
import ReactMarkdown from 'react-markdown';
import rehypeRaw from 'rehype-raw';
import type { ChartPackItem } from '../../lib/api';
import ChartPackRenderer from '../charts/ChartPackRenderer';

interface Props {
  markdown: string;
  charts: ChartPackItem[];
}

type Segment =
  | { type: 'markdown'; content: string }
  | { type: 'charts'; chartIds: string[] };

const CHART_MARKER_RE = /^:::\s*charts?\s+([a-zA-Z0-9_,\s-]+)\s*:::$/gm;

const SECTION_CHART_FALLBACKS: Array<{ heading: string; chartIds: string[] }> = [
  { heading: '## 行业赛道分析', chartIds: ['industry_prosperity', 'peer_comparison'] },
  { heading: '## 财务质量深度核查', chartIds: ['financial_trend', 'cashflow_compare'] },
  { heading: '## 估值定价与预期差分析', chartIds: ['valuation_percentile'] },
  { heading: '## 风险识别与情景分析', chartIds: ['scenario_analysis'] },
];

function hasRenderableChart(chart: ChartPackItem) {
  return chart.series.some((series) => Array.isArray(series.points) && series.points.length > 0);
}

function resolveChartIds(raw: string) {
  return Array.from(
    new Set(
      raw
        .split(/[,\s]+/)
        .map((item) => item.trim())
        .filter(Boolean),
    ),
  );
}

function injectFallbackMarkers(markdown: string, charts: ChartPackItem[]) {
  if (!markdown.trim()) {
    return markdown;
  }

  CHART_MARKER_RE.lastIndex = 0;
  if (CHART_MARKER_RE.test(markdown)) {
    CHART_MARKER_RE.lastIndex = 0;
    return markdown;
  }

  const availableChartIds = new Set(
    charts.filter(hasRenderableChart).map((chart) => chart.chart_id),
  );

  let nextMarkdown = markdown;
  for (const section of SECTION_CHART_FALLBACKS) {
    const chartIds = section.chartIds.filter((chartId) => availableChartIds.has(chartId));
    if (!chartIds.length || !nextMarkdown.includes(section.heading)) {
      continue;
    }

    nextMarkdown = nextMarkdown.replace(
      section.heading,
      `${section.heading}\n\n:::charts ${chartIds.join(',')}:::`,
    );
  }

  return nextMarkdown;
}

function normalizeLegacyFallbackMarkdown(markdown: string) {
  if (!markdown.includes('本次报告生成阶段出现模型限流或响应异常')) {
    return markdown;
  }

  return markdown
    .replace(/\n待验证项：\n(?:- .*\n)+/g, '\n')
    .replace(/\n缺失字段：\n(?:- .*\n)+/g, '\n')
    .replace(/^## 证据闸门与字段约束[\s\S]*?(?=^## )/m, '')
    .replace(/^## 证据包摘要$/m, '## 研究边界与待补证据')
    .replace(/^## 图表包摘要$/m, '## 证据来源与复核入口')
    .replace(/^## 生成说明[\s\S]*$/m, '')
    .trim();
}

function splitMarkdownSegments(markdown: string): Segment[] {
  const segments: Segment[] = [];
  let lastIndex = 0;

  CHART_MARKER_RE.lastIndex = 0;
  for (const match of markdown.matchAll(CHART_MARKER_RE)) {
    const start = match.index ?? 0;
    const marker = match[0];
    const content = markdown.slice(lastIndex, start);

    if (content.trim()) {
      segments.push({ type: 'markdown', content });
    }

    const chartIds = resolveChartIds(match[1] || '');
    if (chartIds.length) {
      segments.push({ type: 'charts', chartIds });
    }

    lastIndex = start + marker.length;
  }

  const tail = markdown.slice(lastIndex);
  if (tail.trim()) {
    segments.push({ type: 'markdown', content: tail });
  }

  if (!segments.length) {
    segments.push({ type: 'markdown', content: markdown });
  }

  return segments;
}

const ReportMarkdown: React.FC<Props> = ({ markdown, charts }) => {
  const normalizedMarkdown = injectFallbackMarkers(normalizeLegacyFallbackMarkdown(markdown), charts);
  const segments = splitMarkdownSegments(normalizedMarkdown);

  return (
    <div className="markdown-content space-y-6">
      {segments.map((segment, index) => {
        if (segment.type === 'charts') {
          const filteredCharts = segment.chartIds
            .map((chartId) => charts.find((chart) => chart.chart_id === chartId))
            .filter((chart): chart is ChartPackItem => Boolean(chart && hasRenderableChart(chart)));

          if (!filteredCharts.length) {
            return null;
          }

          return (
            <div key={`chart-${index}`} className="report-inline-charts">
              <ChartPackRenderer charts={filteredCharts} />
            </div>
          );
        }

        return (
          <ReactMarkdown key={`markdown-${index}`} rehypePlugins={[rehypeRaw]}>
            {segment.content}
          </ReactMarkdown>
        );
      })}
    </div>
  );
};

export default ReportMarkdown;
