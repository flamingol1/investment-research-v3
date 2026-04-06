/**
 * 数据源管理页 - 数据源列表、健康检测、启用/禁用
 */
import { useMemo } from 'react';
import {
  Card,
  Table,
  Tag,
  Button,
  Switch,
  Modal,
  Typography,
  Spin,
  message,
  Tooltip,
  Badge,
} from 'antd';
import {
  ReloadOutlined,
  CheckCircleOutlined,
  WarningOutlined,
  CloseCircleOutlined,
  QuestionCircleOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  listIntelSources,
  updateIntelSource,
  checkSourceHealth,
  checkAllHealth,
  deleteIntelSource,
  type IntelSource,
} from '../lib/api';

const { Title, Text } = Typography;

const STATUS_COLORS = {
  success: '#22c55e',
  warning: '#f59e0b',
  error: '#ef4444',
  muted: '#6b7280',
} as const;

const healthIcon: Record<string, React.ReactNode> = {
  healthy: <CheckCircleOutlined style={{ color: STATUS_COLORS.success }} />,
  degraded: <WarningOutlined style={{ color: STATUS_COLORS.warning }} />,
  down: <CloseCircleOutlined style={{ color: STATUS_COLORS.error }} />,
  unknown: <QuestionCircleOutlined style={{ color: STATUS_COLORS.muted }} />,
};

const healthTag: Record<string, { color: string; label: string }> = {
  healthy: { color: 'success', label: '健康' },
  degraded: { color: 'warning', label: '降级' },
  down: { color: 'error', label: '离线' },
  unknown: { color: 'default', label: '未知' },
};

function IntelSources() {
  const queryClient = useQueryClient();

  const { data: sources = [], isLoading } = useQuery({
    queryKey: ['intel-sources'],
    queryFn: listIntelSources,
  });

  const toggleMutation = useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      updateIntelSource(name, { enabled }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['intel-sources'] });
      message.success('状态已更新');
    },
    onError: (err: unknown) => {
      message.error('操作失败');
      console.error('[IntelSources] toggle failed:', err);
    },
  });

  const healthMutation = useMutation({
    mutationFn: checkSourceHealth,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['intel-sources'] });
      message.success('健康检查完成');
    },
    onError: (err: unknown) => {
      message.error('健康检查失败');
      console.error('[IntelSources] health check failed:', err);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: deleteIntelSource,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['intel-sources'] });
      message.success('数据源已删除');
    },
    onError: (err: unknown) => {
      message.error('删除失败');
      console.error('[IntelSources] delete failed:', err);
    },
  });

  const checkAllMutation = useMutation({
    mutationFn: checkAllHealth,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['intel-sources'] });
      message.success('全部健康检查完成');
    },
    onError: (err: unknown) => {
      message.error('批量检查失败');
      console.error('[IntelSources] check-all failed:', err);
    },
  });

  const handleDelete = (record: IntelSource) => {
    Modal.confirm({
      title: `确认删除数据源 "${record.display_name}"?`,
      content: '删除后需要重新添加',
      okText: '删除',
      cancelText: '取消',
      okButtonProps: { danger: true },
      onOk: () => deleteMutation.mutateAsync(record.name),
    });
  };

  const columns = useMemo(() => [
    {
      title: '数据源',
      dataIndex: 'display_name',
      key: 'display_name',
      render: (name: string, record: IntelSource) => (
        <div>
          <Text strong style={{ color: 'var(--color-text-primary)' }}>{name}</Text>
          <br />
          <Text style={{ color: 'var(--color-text-muted)', fontSize: 12 }}>{record.description}</Text>
        </div>
      ),
    },
    {
      title: '标识',
      dataIndex: 'name',
      key: 'name',
      width: 120,
      render: (name: string) => (
        <Tag style={{ background: 'var(--color-bg-elevated)', borderColor: 'var(--color-border)' }}>
          {name}
        </Tag>
      ),
    },
    {
      title: '优先级',
      dataIndex: 'priority',
      key: 'priority',
      width: 80,
      sorter: (a: IntelSource, b: IntelSource) => a.priority - b.priority,
      render: (p: number) => (
        <Badge count={p} style={{ backgroundColor: 'var(--color-brand)' }} />
      ),
    },
    {
      title: '健康状态',
      dataIndex: 'health_status',
      key: 'health_status',
      width: 120,
      render: (status: string) => {
        const info = healthTag[status] || healthTag.unknown;
        return (
          <Tag icon={healthIcon[status]} color={info.color}>
            {info.label}
          </Tag>
        );
      },
    },
    {
      title: '启用',
      dataIndex: 'enabled',
      key: 'enabled',
      width: 80,
      render: (enabled: boolean, record: IntelSource) => (
        <Switch
          checked={enabled}
          size="small"
          onChange={(v) => toggleMutation.mutate({ name: record.name, enabled: v })}
          aria-label={`切换 ${record.display_name} 启用状态`}
        />
      ),
    },
    {
      title: '最近错误',
      dataIndex: 'last_error',
      key: 'last_error',
      ellipsis: true,
      render: (err: string) =>
        err ? (
          <Tooltip title={err}>
            <Text style={{ color: STATUS_COLORS.error, fontSize: 12 }}>{err}</Text>
          </Tooltip>
        ) : (
          <Text style={{ color: 'var(--color-text-muted)', fontSize: 12 }}>-</Text>
        ),
    },
    {
      title: '操作',
      key: 'actions',
      width: 160,
      render: (_: unknown, record: IntelSource) => (
        <div className="flex gap-2">
          <Button
            size="small"
            icon={<ThunderboltOutlined />}
            onClick={() => healthMutation.mutate(record.name)}
            loading={healthMutation.isPending && healthMutation.variables === record.name}
          >
            检测
          </Button>
          <Button
            size="small"
            danger
            onClick={() => handleDelete(record)}
          >
            删除
          </Button>
        </div>
      ),
    },
  ], [toggleMutation, healthMutation]);

  const healthyCount = sources.filter((s) => s.health_status === 'healthy').length;

  return (
    <div className="space-y-6 max-w-7xl">
      <div className="flex items-center justify-between">
        <div>
          <Title level={4} style={{ color: 'var(--color-text-primary)', margin: 0 }}>
            数据源管理
          </Title>
          <Text style={{ color: 'var(--color-text-secondary)' }}>
            管理和监控情报中心的数据采集来源
          </Text>
        </div>
        <Button
          icon={<ReloadOutlined />}
          onClick={() => checkAllMutation.mutate()}
          loading={checkAllMutation.isPending}
        >
          全量健康检查
        </Button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card size="small">
          <div className="flex items-center justify-between">
            <Text style={{ color: 'var(--color-text-secondary)' }}>总数据源</Text>
            <Text strong style={{ color: 'var(--color-text-primary)', fontSize: 24 }}>{sources.length}</Text>
          </div>
        </Card>
        <Card size="small">
          <div className="flex items-center justify-between">
            <Text style={{ color: 'var(--color-text-secondary)' }}>健康</Text>
            <Text strong style={{ color: STATUS_COLORS.success, fontSize: 24 }}>{healthyCount}</Text>
          </div>
        </Card>
        <Card size="small">
          <div className="flex items-center justify-between">
            <Text style={{ color: 'var(--color-text-secondary)' }}>已启用</Text>
            <Text strong style={{ color: 'var(--color-brand)', fontSize: 24 }}>
              {sources.filter((s) => s.enabled).length}
            </Text>
          </div>
        </Card>
      </div>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spin /></div>
        ) : (
          <Table
            dataSource={sources}
            columns={columns}
            rowKey="id"
            pagination={false}
            size="middle"
            rowClassName={() => 'hover:bg-white/5'}
          />
        )}
      </Card>
    </div>
  );
}

export default IntelSources;
