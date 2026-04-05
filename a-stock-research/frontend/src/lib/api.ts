/**
 * API客户端 - 统一的后端通信层
 */
import axios from 'axios';

const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
});

// ============================================================
// 类型定义
// ============================================================

export interface ResearchRequest {
  stock_code: string;
  depth: 'quick' | 'standard' | 'deep';
}

export interface ApiResponse<T = unknown> {
  success: boolean;
  message: string;
  data?: T;
}

export interface ResearchStatus {
  task_id: string;
  stock_code: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  progress: number;
  stage: string;
  current_agent: string;
  started_at: string | null;
  completed_at: string | null;
  report: ReportSummary | null;
  errors: string[];
}

export interface ReportSummary {
  stock_code: string;
  stock_name: string;
  report_date: string;
  depth: string;
  recommendation: string;
  risk_level: string;
  target_price_low: number | null;
  target_price_high: number | null;
  current_price: number | null;
  upside_pct: number | null;
  has_full_report: boolean;
  agents_completed: string[];
}

export interface ReportDetail {
  stock_code: string;
  stock_name: string;
  report_date: string;
  markdown: string;
  conclusion: InvestmentConclusion | null;
  agents_completed: string[];
  agents_skipped: string[];
  errors: string[];
}

export interface InvestmentConclusion {
  recommendation: string;
  confidence_level: string;
  target_price_low: number | null;
  target_price_high: number | null;
  current_price: number | null;
  upside_pct: number | null;
  risk_level: string;
  key_reasons_buy: string[];
  key_reasons_sell: string[];
  key_assumptions: string[];
  monitoring_points: string[];
  position_advice: string;
  holding_period: string;
  stop_loss_price: number | null;
  conclusion_summary: string;
}

export interface WatchListItem {
  stock_code: string;
  stock_name: string;
  recommendation: string;
  added_at: string | null;
  last_updated_at: string | null;
  last_report_date: string | null;
  status: string;
  notes: string;
}

export interface WatchListResponse {
  items: WatchListItem[];
  total: number;
  updated_at: string | null;
}

export interface HistoryEntry {
  stock_code: string;
  stock_name: string;
  research_date: string;
  depth: string;
  recommendation: string | null;
  risk_level: string | null;
  target_price_low: number | null;
  target_price_high: number | null;
  current_price: number | null;
  agents_completed: string[];
}

export interface HistoryResponse {
  stock_code: string;
  stock_name: string;
  entries: HistoryEntry[];
}

export interface SearchRequest {
  query: string;
  category?: string;
  num_results?: number;
}

export interface SearchItem {
  document: string;
  stock_code: string;
  stock_name: string;
  category: string;
  date: string;
  similarity: number;
}

export interface SearchResponse {
  query: string;
  results: SearchItem[];
  total: number;
}

// ============================================================
// WebSocket 进度消息
// ============================================================

export interface ProgressMessage {
  stage: string;
  agent: string;
  status: string;
  progress: number;
  message: string;
}

export function connectResearchWS(
  taskId: string,
  onMessage: (msg: ProgressMessage) => void,
  onClose?: () => void,
): WebSocket {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(`${protocol}//${window.location.host}/ws/research/${taskId}`);

  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type !== 'ping') {
        onMessage(msg);
      }
    } catch {
      // ignore parse errors
    }
  };

  ws.onclose = () => onClose?.();

  return ws;
}

// ============================================================
// API 调用函数
// ============================================================

export async function startResearch(req: ResearchRequest): Promise<ApiResponse<{ task_id: string }>> {
  const { data } = await api.post('/research', req);
  return data;
}

export async function getResearchStatus(taskId: string): Promise<ResearchStatus> {
  const { data } = await api.get(`/research/${taskId}`);
  return data;
}

export async function listReports(): Promise<ReportSummary[]> {
  const { data } = await api.get('/reports');
  return data;
}

export async function getReport(stockCode: string, date: string): Promise<ReportDetail> {
  const { data } = await api.get(`/reports/${stockCode}/${date}`);
  return data;
}

export async function getWatchList(): Promise<WatchListResponse> {
  const { data } = await api.get('/watch');
  return data;
}

export async function addToWatch(stockCode: string, stockName = ''): Promise<ApiResponse> {
  const { data } = await api.post('/watch', { stock_code: stockCode, stock_name: stockName });
  return data;
}

export async function removeFromWatch(stockCode: string): Promise<ApiResponse> {
  const { data } = await api.delete(`/watch/${stockCode}`);
  return data;
}

export async function getHistory(stockCode: string): Promise<HistoryResponse> {
  const { data } = await api.get(`/history/${stockCode}`);
  return data;
}

export async function searchKnowledge(req: SearchRequest): Promise<SearchResponse> {
  const { data } = await api.post('/search', req);
  return data;
}

export async function triggerUpdate(stockCode: string): Promise<ApiResponse> {
  const { data } = await api.post(`/update/${stockCode}`);
  return data;
}

export async function healthCheck(): Promise<{ status: string; version: string }> {
  const { data } = await api.get('/health');
  return data;
}
