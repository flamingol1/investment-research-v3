/**
 * Frontend API client.
 */
import axios from 'axios';

const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
});

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
  message: string;
  stage_detail: ProgressDetail | null;
  data_summary: ProgressMetric[];
  recent_events: ResearchEvent[];
  completed_agents: string[];
  active_agents: string[];
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

export interface ChartSeries {
  name: string;
  points: Array<{ x: string | number; y: number }>;
}

export interface ChartPackItem {
  chart_id: string;
  title: string;
  chart_type: string;
  unit: string;
  summary: string;
  series: ChartSeries[];
}

export interface EvidencePackItem {
  category: string;
  title: string;
  source: string;
  url: string;
  excerpt: string;
  fields: string[];
  reference_date: string;
}

export interface QualityGateDecision {
  blocked: boolean;
  gate_type: string;
  core_evidence_score: number;
  blocking_fields: string[];
  weak_fields: string[];
  reasons: string[];
  consistency_notes: string[];
  coverage_ratio: number;
  company_cross_confidence: number;
  peer_verified: number;
}

export interface ReportDetail {
  stock_code: string;
  stock_name: string;
  report_date: string;
  depth: string;
  markdown: string;
  conclusion: InvestmentConclusion | null;
  chart_pack: ChartPackItem[];
  evidence_pack: EvidencePackItem[];
  agents_completed: string[];
  agents_skipped: string[];
  quality_gate: QualityGateDecision | null;
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
  warning?: string;
}

export interface SecurityLookupItem {
  stock_code: string;
  stock_name: string;
  exchange: string;
  has_report: boolean;
  in_watchlist: boolean;
}

export interface SecurityLookupResponse {
  query: string;
  items: SecurityLookupItem[];
  total: number;
  source: string;
  fallback: boolean;
}

export interface UpdateResponse {
  stock_code: string;
  status: 'success' | 'failed';
  message: string;
  changes: Record<string, number>;
  duration_seconds: number;
  errors: string[];
}

export interface ProgressMessage {
  stage: string;
  agent: string;
  status: string;
  progress: number;
  message: string;
  stage_detail?: ProgressDetail | null;
  data_summary?: ProgressMetric[];
  recent_events?: ResearchEvent[];
  completed_agents?: string[];
  active_agents?: string[];
  event?: ResearchEvent | null;
  timestamp?: string;
}

export interface ProgressMetric {
  key: string;
  label: string;
  value: string;
  tone: 'default' | 'info' | 'success' | 'warning' | 'danger';
}

export interface ProgressDetail {
  headline: string;
  note: string;
  metrics: ProgressMetric[];
  bullets: string[];
}

export interface ResearchEvent {
  id: number;
  stage: string;
  agent: string;
  status: 'running' | 'completed' | 'failed';
  message: string;
  created_at: string;
  detail?: ProgressDetail | null;
}

export function connectResearchWS(
  taskId: string,
  onMessage: (msg: ProgressMessage) => void,
  onClose?: () => void,
): WebSocket {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(`${protocol}//${window.location.host}/api/ws/research/${taskId}`);

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

export async function searchSecurities(query: string, limit = 8): Promise<SecurityLookupResponse> {
  const { data } = await api.get('/securities/search', {
    params: {
      q: query,
      limit,
    },
  });
  return data;
}

export async function triggerUpdate(stockCode: string): Promise<UpdateResponse> {
  const { data } = await api.post(`/update/${stockCode}`);
  return data;
}

export async function healthCheck(): Promise<{ status: string; version: string }> {
  const { data } = await api.get('/health');
  return data;
}

// ============================================================
// 情报中心 (Intelligence Hub) API
// ============================================================

export interface IntelSource {
  id: number;
  name: string;
  display_name: string;
  description: string;
  enabled: boolean;
  priority: number;
  config_json: string;
  health_status: string;
  last_health_check: string | null;
  last_error: string;
  created_at: string;
  updated_at: string;
}

export interface IntelSourceUpdate {
  display_name?: string;
  description?: string;
  enabled?: boolean;
  priority?: number;
  config_json?: Record<string, unknown>;
}

export interface IntelTask {
  id: number;
  name: string;
  task_type: string;
  target: string;
  schedule_type: string;
  schedule_expr: string;
  enabled: boolean;
  source_id: number | null;
  status: string;
  last_run_at: string | null;
  next_run_at: string | null;
  success_count: number;
  fail_count: number;
  created_at: string;
  updated_at: string;
}

export interface IntelTaskCreate {
  name: string;
  task_type: string;
  target: string;
  schedule_type?: string;
  schedule_expr?: string;
  enabled?: boolean;
  source_name?: string | null;
}

export interface IntelTaskUpdate {
  name?: string;
  task_type?: string;
  schedule_type?: string;
  schedule_expr?: string;
  enabled?: boolean;
  source_name?: string | null;
}

export interface CollectionLogItem {
  id: number;
  source_name: string;
  target?: string;
  data_type?: string;
  status: string;
  records_fetched: number;
  records_stored: number;
  error_message: string;
  duration_ms: number;
  started_at: string | null;
  completed_at?: string | null;
}

export interface CollectResult {
  data_type: string;
  source: string;
  status: string;
  records_fetched: number;
  duration_ms: number;
  error: string | null;
}

export interface CollectStockResponse {
  stock_code: string;
  results: CollectResult[];
  success_count: number;
  failed_count: number;
}

export interface IntelArchive {
  id: number;
  stock_code: string;
  stock_name: string;
  category: string;
  source_name: string;
  data_date: string | null;
  title: string;
  summary: string;
  tags: string;
  indexed: boolean;
  created_at: string;
}

export interface IntelArchiveListResponse {
  items: IntelArchive[];
  total: number;
  page: number;
  page_size: number;
}

export interface IntelStats {
  sources: { total: number; healthy: number };
  archives: Record<string, unknown>;
  data_types: number;
}

export interface KnowledgeStats {
  archives: Record<string, unknown>;
  vector_collections: string[];
}

// --- Data Sources ---

export async function listIntelSources(): Promise<IntelSource[]> {
  const { data } = await api.get('/intel/sources');
  return data;
}

export async function getIntelSource(name: string): Promise<IntelSource> {
  const { data } = await api.get(`/intel/sources/${name}`);
  return data;
}

export async function updateIntelSource(name: string, update: IntelSourceUpdate): Promise<IntelSource> {
  const { data } = await api.put(`/intel/sources/${name}`, update);
  return data;
}

export async function deleteIntelSource(name: string): Promise<{ message: string }> {
  const { data } = await api.delete(`/intel/sources/${name}`);
  return data;
}

export async function checkSourceHealth(name: string): Promise<{ name: string; status: string; error?: string }> {
  const { data } = await api.post(`/intel/sources/${name}/health`);
  return data;
}

export async function checkAllHealth(): Promise<Record<string, { status: string; error?: string }>> {
  const { data } = await api.post('/intel/sources/check-all');
  return data;
}

// --- Collection Tasks ---

export async function listIntelTasks(): Promise<IntelTask[]> {
  const { data } = await api.get('/intel/tasks');
  return data;
}

export async function createIntelTask(task: IntelTaskCreate): Promise<IntelTask> {
  const { data } = await api.post('/intel/tasks', task);
  return data;
}

export async function updateIntelTask(taskId: number, update: IntelTaskUpdate): Promise<IntelTask> {
  const { data } = await api.put(`/intel/tasks/${taskId}`, update);
  return data;
}

export async function deleteIntelTask(taskId: number): Promise<{ message: string }> {
  const { data } = await api.delete(`/intel/tasks/${taskId}`);
  return data;
}

export async function runIntelTask(taskId: number): Promise<{ task_id: number; results: CollectResult[] }> {
  const { data } = await api.post(`/intel/tasks/${taskId}/run`);
  return data;
}

export async function getTaskLogs(taskId: number, limit = 50): Promise<CollectionLogItem[]> {
  const { data } = await api.get(`/intel/tasks/${taskId}/logs`, { params: { limit } });
  return data;
}

// --- Collection Execution ---

export async function collectStock(code: string, dataTypes?: string[]): Promise<CollectStockResponse> {
  const params = dataTypes ? { data_types: dataTypes } : {};
  const { data } = await api.post(`/intel/collect/stock/${code}`, null, { params });
  return data;
}

export async function getCollectionLogs(target?: string, limit = 50): Promise<CollectionLogItem[]> {
  const { data } = await api.get('/intel/logs', { params: { target, limit } });
  return data;
}

// --- Archives ---

export async function listArchives(params?: {
  stock_code?: string;
  category?: string;
  keyword?: string;
  source_name?: string;
  page?: number;
  page_size?: number;
}): Promise<IntelArchiveListResponse> {
  const { data } = await api.get('/intel/archives', { params });
  return data;
}

export async function getArchiveContent(archiveId: number): Promise<{ id: number; content: Record<string, unknown> }> {
  const { data } = await api.get(`/intel/archives/${archiveId}`);
  return data;
}

export async function deleteArchive(archiveId: number): Promise<{ message: string }> {
  const { data } = await api.delete(`/intel/archives/${archiveId}`);
  return data;
}

export async function getArchiveStats(): Promise<Record<string, unknown>> {
  const { data } = await api.get('/intel/archives/stats');
  return data;
}

// --- Knowledge ---

export async function getIntelStats(): Promise<IntelStats> {
  const { data } = await api.get('/intel/stats');
  return data;
}

export async function getDataTypes(): Promise<Record<string, { display_name: string; description: string; category: string }>> {
  const { data } = await api.get('/intel/data-types');
  return data;
}

export async function getKnowledgeStats(): Promise<KnowledgeStats> {
  const { data } = await api.get('/intel/knowledge/stats');
  return data;
}

export async function searchKnowledgeIntel(params: {
  keyword: string;
  stock_code?: string;
  category?: string;
  page?: number;
  page_size?: number;
}): Promise<{ items: IntelArchive[]; total: number }> {
  const { data } = await api.get('/intel/knowledge/search', { params });
  return data;
}

export async function rebuildKnowledge(): Promise<{ message: string; indexed_count: number; total_count: number }> {
  const { data } = await api.post('/intel/knowledge/rebuild');
  return data;
}

// --- Streaming Collection (SSE) ---

export interface CollectStreamResponse {
  collect_id: string;
  stock_code: string;
  total_steps: number;
}

export interface StepStartEvent {
  data_type: string;
  display_name: string;
  step: number;
  total: number;
}

export interface StepCompleteEvent {
  data_type: string;
  source: string;
  status: string;
  records_fetched: number;
  duration_ms: number;
  error: string | null;
  step: number;
  total: number;
  progress: number;
}

export interface CollectDoneEvent {
  success_count: number;
  failed_count: number;
}

export async function collectStockStream(code: string, dataTypes?: string[]): Promise<CollectStreamResponse> {
  const params = dataTypes ? { data_types: dataTypes } : {};
  const { data } = await api.post(`/intel/collect/stock/${code}/stream`, null, { params, timeout: 5000 });
  return data;
}

export async function runIntelTaskStream(taskId: number): Promise<CollectStreamResponse> {
  const { data } = await api.post(`/intel/tasks/${taskId}/stream`, null, { timeout: 5000 });
  return data;
}
