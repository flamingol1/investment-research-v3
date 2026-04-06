/**
 * 采集任务管理页 - 任务列表、创建任务、一键采集(实时进度)、日志查看
 */
import { useState, useMemo, useCallback, useEffect } from 'react';
import {
  Card,
  Table,
  Tag,
  Button,
  Modal,
  Form,
  Input,
  Select,
  Space,
  Typography,
  Spin,
  message,
  Switch,
  Drawer,
  List,
  Alert,
} from 'antd';
import {
  PlusOutlined,
  PlayCircleOutlined,
  DeleteOutlined,
  HistoryOutlined,
  ThunderboltOutlined,
  UnorderedListOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  listIntelTasks,
  createIntelTask,
  updateIntelTask,
  deleteIntelTask,
  runIntelTaskStream,
  collectStockStream,
  getTaskLogs,
  getDataTypes,
  type IntelTask,
  type CollectionLogItem,
} from '../lib/api';
import { useCollectionProgress } from '../lib/useCollectionProgress';
import CollectionProgressPanel from '../components/CollectionProgressPanel';

const { Title, Text } = Typography;

const STATUS_COLORS = {
  success: '#22c55e',
  error: '#ef4444',
  warning: '#f59e0b',
} as const;

const statusTag: Record<string, { color: string; label: string }> = {
  idle: { color: 'default', label: '空闲' },
  running: { color: 'processing', label: '运行中' },
  success: { color: 'success', label: '成功' },
  failed: { color: 'error', label: '失败' },
};

function IntelCollect() {
  const queryClient = useQueryClient();
  const [createOpen, setCreateOpen] = useState(false);
  const [collectOpen, setCollectOpen] = useState(false);
  const [logDrawerOpen, setLogDrawerOpen] = useState(false);
  const [selectedTaskId, setSelectedTaskId] = useState<number | null>(null);
  const [collectCode, setCollectCode] = useState('');
  const [form] = Form.useForm();

  // 流式进度状态
  const [collectStreamId, setCollectStreamId] = useState<string | null>(null);
  const [collectStockCode, setCollectStockCode] = useState('');
  const [taskStreamId, setTaskStreamId] = useState<string | null>(null);
  const [taskStreamCode, setTaskStreamCode] = useState('');

  // SSE 进度 hooks
  const collectProgress = useCollectionProgress(collectStreamId);
  const taskProgress = useCollectionProgress(taskStreamId);

  const { data: tasks = [], isLoading } = useQuery({
    queryKey: ['intel-tasks'],
    queryFn: listIntelTasks,
  });

  const { data: dataTypes = {} } = useQuery({
    queryKey: ['intel-data-types'],
    queryFn: getDataTypes,
  });

  const { data: logs = [], isLoading: logsLoading } = useQuery({
    queryKey: ['intel-task-logs', selectedTaskId],
    queryFn: () => (selectedTaskId ? getTaskLogs(selectedTaskId) : Promise.resolve([])),
    enabled: selectedTaskId !== null,
  });

  // 完成时刷新列表
  useEffect(() => {
    if (collectProgress.phase === 'completed' || collectProgress.phase === 'failed') {
      queryClient.invalidateQueries({ queryKey: ['intel-tasks'] });
    }
  }, [collectProgress.phase, queryClient]);

  useEffect(() => {
    if (taskProgress.phase === 'completed' || taskProgress.phase === 'failed') {
      queryClient.invalidateQueries({ queryKey: ['intel-tasks'] });
    }
  }, [taskProgress.phase, queryClient]);

  // 一键采集 — 流式
  const collectStreamMutation = useMutation({
    mutationFn: (code: string) => collectStockStream(code),
    onSuccess: (data) => {
      setCollectStreamId(data.collect_id);
    },
    onError: (err: unknown) => {
      message.error('启动采集失败');
      console.error('[IntelCollect] stream collect failed:', err);
    },
  });

  // 任务执行 — 流式
  const runStreamMutation = useMutation({
    mutationFn: runIntelTaskStream,
    onSuccess: (data) => {
      setTaskStreamId(data.collect_id);
      setTaskStreamCode(data.stock_code);
    },
    onError: (err: unknown) => {
      message.error('启动任务失败');
      console.error('[IntelCollect] stream run failed:', err);
    },
  });

  const createMutation = useMutation({
    mutationFn: createIntelTask,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['intel-tasks'] });
      message.success('任务已创建');
      setCreateOpen(false);
      form.resetFields();
    },
    onError: (err: unknown) => {
      message.error('创建失败');
      console.error('[IntelCollect] create failed:', err);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: deleteIntelTask,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['intel-tasks'] });
      message.success('任务已删除');
    },
    onError: (err: unknown) => {
      message.error('删除失败');
      console.error('[IntelCollect] delete failed:', err);
    },
  });

  const toggleMutation = useMutation({
    mutationFn: ({ id, enabled }: { id: number; enabled: boolean }) =>
      updateIntelTask(id, { enabled }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['intel-tasks'] });
    },
    onError: (err: unknown) => {
      message.error('操作失败');
      console.error('[IntelCollect] toggle failed:', err);
    },
  });

  const handleCollect = useCallback(() => {
    if (!collectCode.trim()) {
      message.warning('请输入股票代码');
      return;
    }
    setCollectStockCode(collectCode.trim());
    setCollectStreamId(null); // 重置进度
    collectStreamMutation.mutate(collectCode.trim());
  }, [collectCode, collectStreamMutation]);

  const handleViewLogs = useCallback((taskId: number) => {
    setSelectedTaskId(taskId);
    setLogDrawerOpen(true);
  }, []);

  const handleDelete = useCallback((task: IntelTask) => {
    Modal.confirm({
      title: `确认删除任务 "${task.name}"?`,
      okText: '删除',
      cancelText: '取消',
      okButtonProps: { danger: true },
      onOk: () => deleteMutation.mutateAsync(task.id),
    });
  }, [deleteMutation]);

  const handleRunTask = useCallback((record: IntelTask) => {
    setTaskStreamId(null);
    setTaskStreamCode(record.target);
    runStreamMutation.mutate(record.id);
  }, [runStreamMutation]);

  const handleCollectModalClose = useCallback(() => {
    setCollectOpen(false);
    setCollectStreamId(null);
    setCollectStockCode('');
    setCollectCode('');
    collectStreamMutation.reset();
  }, [collectStreamMutation]);

  const taskTypeOptions = useMemo(() =>
    Object.entries(dataTypes).map(([key, val]) => ({
      label: `${val.display_name} (${key})`,
      value: key,
    })),
    [dataTypes],
  );

  const columns = useMemo(() => [
    {
      title: '任务名称',
      dataIndex: 'name',
      key: 'name',
      render: (name: string) => (
        <Text strong style={{ color: 'var(--color-text-primary)' }}>{name}</Text>
      ),
    },
    {
      title: '类型',
      dataIndex: 'task_type',
      key: 'task_type',
      width: 100,
      render: (t: string) => <Tag>{t}</Tag>,
    },
    {
      title: '目标',
      dataIndex: 'target',
      key: 'target',
      width: 100,
    },
    {
      title: '调度',
      key: 'schedule',
      width: 120,
      render: (_: unknown, r: IntelTask) => (
        <div>
          <Tag>{r.schedule_type}</Tag>
          {r.schedule_expr && (
            <Text style={{ color: 'var(--color-text-muted)', fontSize: 11 }}>{r.schedule_expr}</Text>
          )}
        </div>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 90,
      render: (s: string) => {
        const info = statusTag[s] || statusTag.idle;
        return <Tag color={info.color}>{info.label}</Tag>;
      },
    },
    {
      title: '成功/失败',
      key: 'counts',
      width: 100,
      render: (_: unknown, r: IntelTask) => (
        <span>
          <Text style={{ color: STATUS_COLORS.success }}>{r.success_count}</Text>
          <Text style={{ color: 'var(--color-text-muted)' }}>/</Text>
          <Text style={{ color: STATUS_COLORS.error }}>{r.fail_count}</Text>
        </span>
      ),
    },
    {
      title: '启用',
      dataIndex: 'enabled',
      key: 'enabled',
      width: 70,
      render: (enabled: boolean, record: IntelTask) => (
        <Switch
          checked={enabled}
          size="small"
          onChange={(v) => toggleMutation.mutate({ id: record.id, enabled: v })}
          aria-label={`切换 ${record.name} 启用状态`}
        />
      ),
    },
    {
      title: '操作',
      key: 'actions',
      width: 180,
      render: (_: unknown, record: IntelTask) => (
        <Space size="small">
          <Button
            size="small"
            type="primary"
            icon={<PlayCircleOutlined />}
            onClick={() => handleRunTask(record)}
            loading={runStreamMutation.isPending && runStreamMutation.variables === record.id}
          >
            执行
          </Button>
          <Button
            size="small"
            icon={<HistoryOutlined />}
            onClick={() => handleViewLogs(record.id)}
          >
            日志
          </Button>
          <Button
            size="small"
            danger
            icon={<DeleteOutlined />}
            onClick={() => handleDelete(record)}
          />
        </Space>
      ),
    },
  ], [runStreamMutation, toggleMutation, handleRunTask, handleViewLogs, handleDelete]);

  const successRate = useMemo(() => {
    if (tasks.length === 0) return 0;
    const successes = tasks.reduce((sum, t) => sum + t.success_count, 0);
    const total = Math.max(1, tasks.reduce((sum, t) => sum + t.success_count + t.fail_count, 0));
    return Math.round((successes / total) * 100);
  }, [tasks]);

  return (
    <div className="space-y-6 max-w-7xl">
      <div className="flex items-center justify-between">
        <div>
          <Title level={4} style={{ color: 'var(--color-text-primary)', margin: 0 }}>
            采集任务管理
          </Title>
          <Text style={{ color: 'var(--color-text-secondary)' }}>
            管理数据采集任务，手动触发或定时调度
          </Text>
        </div>
        <Space>
          <Button icon={<ThunderboltOutlined />} onClick={() => setCollectOpen(true)}>
            一键采集
          </Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateOpen(true)}>
            创建任务
          </Button>
        </Space>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card size="small">
          <div className="flex items-center justify-between">
            <Text style={{ color: 'var(--color-text-secondary)' }}>总任务</Text>
            <Text strong style={{ color: 'var(--color-text-primary)', fontSize: 24 }}>{tasks.length}</Text>
          </div>
        </Card>
        <Card size="small">
          <div className="flex items-center justify-between">
            <Text style={{ color: 'var(--color-text-secondary)' }}>已启用</Text>
            <Text strong style={{ color: 'var(--color-brand)', fontSize: 24 }}>
              {tasks.filter((t) => t.enabled).length}
            </Text>
          </div>
        </Card>
        <Card size="small">
          <div className="flex items-center justify-between">
            <Text style={{ color: 'var(--color-text-secondary)' }}>成功率</Text>
            <Text strong style={{ color: successRate >= 80 ? STATUS_COLORS.success : STATUS_COLORS.warning, fontSize: 24 }}>
              {successRate}%
            </Text>
          </div>
        </Card>
      </div>

      {/* 任务执行实时进度 */}
      {taskStreamId && taskProgress.phase !== 'idle' && (
        <Card
          size="small"
          title={<span><PlayCircleOutlined style={{ marginRight: 8 }} />任务执行进度 — {taskStreamCode}</span>}
          extra={
            (taskProgress.phase === 'completed' || taskProgress.phase === 'failed') && (
              <Button size="small" onClick={() => { setTaskStreamId(null); setTaskStreamCode(''); }}>
                关闭
              </Button>
            )
          }
        >
          <CollectionProgressPanel
            steps={taskProgress.steps}
            currentStep={taskProgress.currentStep}
            totalSteps={taskProgress.totalSteps}
            phase={taskProgress.phase}
            doneEvent={taskProgress.doneEvent}
            error={taskProgress.error}
            stockCode={taskStreamCode}
          />
        </Card>
      )}

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spin /></div>
        ) : (
          <Table
            dataSource={tasks}
            columns={columns}
            rowKey="id"
            pagination={{ pageSize: 10 }}
            size="middle"
          />
        )}
      </Card>

      <Modal
        title="创建采集任务"
        open={createOpen}
        onCancel={() => { setCreateOpen(false); form.resetFields(); }}
        onOk={() => form.submit()}
        confirmLoading={createMutation.isPending}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={(values) => createMutation.mutate(values)}
        >
          <Form.Item name="name" label="任务名称" rules={[{ required: true, message: '请输入名称' }]}>
            <Input placeholder="例: 每日行情采集" />
          </Form.Item>
          <Form.Item name="task_type" label="采集类型" rules={[{ required: true, message: '请选择类型' }]}>
            <Select options={taskTypeOptions} placeholder="选择数据类型" />
          </Form.Item>
          <Form.Item name="target" label="采集目标" rules={[{ required: true, message: '请输入目标' }]}>
            <Input placeholder="股票代码，如 300358" />
          </Form.Item>
          <div className="grid grid-cols-2 gap-4">
            <Form.Item name="schedule_type" label="调度方式" initialValue="manual">
              <Select options={[
                { label: '手动', value: 'manual' },
                { label: '定时', value: 'cron' },
                { label: '间隔', value: 'interval' },
                { label: '单次', value: 'once' },
              ]} />
            </Form.Item>
            <Form.Item name="schedule_expr" label="调度表达式">
              <Input placeholder="cron 或秒数" />
            </Form.Item>
          </div>
        </Form>
      </Modal>

      <Modal
        title="一键采集"
        open={collectOpen}
        onCancel={handleCollectModalClose}
        footer={null}
        width={640}
      >
        <Space.Compact className="w-full mb-4">
          <Input
            placeholder="输入股票代码，如 300358"
            value={collectCode}
            onChange={(e) => setCollectCode(e.target.value)}
            onPressEnter={handleCollect}
            size="large"
            disabled={collectProgress.phase === 'running'}
          />
          <Button
            type="primary"
            size="large"
            onClick={handleCollect}
            loading={collectStreamMutation.isPending || collectProgress.phase === 'running'}
            disabled={collectProgress.phase === 'running'}
          >
            开始采集
          </Button>
        </Space.Compact>

        {collectStreamMutation.isError && !collectStreamId && (
          <Alert type="error" message="启动采集失败，请重试" className="mb-4" />
        )}

        {collectStreamId && (
          <CollectionProgressPanel
            steps={collectProgress.steps}
            currentStep={collectProgress.currentStep}
            totalSteps={collectProgress.totalSteps}
            phase={collectProgress.phase}
            doneEvent={collectProgress.doneEvent}
            error={collectProgress.error}
            stockCode={collectStockCode}
          />
        )}
      </Modal>

      <Drawer
        title={<span><UnorderedListOutlined /> 执行日志</span>}
        open={logDrawerOpen}
        onClose={() => { setLogDrawerOpen(false); setSelectedTaskId(null); }}
        width={640}
      >
        {logsLoading ? (
          <div className="flex justify-center py-12"><Spin /></div>
        ) : logs.length === 0 ? (
          <Text style={{ color: 'var(--color-text-muted)' }}>暂无日志记录</Text>
        ) : (
          <List
            size="small"
            dataSource={logs}
            renderItem={(log: CollectionLogItem) => (
              <List.Item>
                <div className="w-full">
                  <div className="flex items-center justify-between mb-1">
                    <div className="flex items-center gap-2">
                      <Tag>{log.data_type || '-'}</Tag>
                      <Text style={{ color: 'var(--color-text-secondary)', fontSize: 12 }}>
                        {log.source_name}
                      </Text>
                    </div>
                    <Tag color={log.status === 'success' ? 'success' : log.status === 'partial' ? 'warning' : 'error'}>
                      {log.status}
                    </Tag>
                  </div>
                  <div className="flex items-center gap-4 text-xs" style={{ color: 'var(--color-text-muted)' }}>
                    <span>获取 {log.records_fetched} 条</span>
                    <span>{log.duration_ms}ms</span>
                    {log.started_at && <span>{log.started_at}</span>}
                  </div>
                  {log.error_message && (
                    <Text style={{ color: STATUS_COLORS.error, fontSize: 12 }} className="mt-1 block">
                      {log.error_message}
                    </Text>
                  )}
                </div>
              </List.Item>
            )}
          />
        )}
      </Drawer>
    </div>
  );
}

export default IntelCollect;
