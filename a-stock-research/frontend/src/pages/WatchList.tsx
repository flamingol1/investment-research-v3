/**
 * 监控列表页面
 */
import React, { useState } from 'react';
import {
  Card, Table, Button, Modal, Input, Tag, Space, Popconfirm, message, Typography,
} from 'antd';
import {
  PlusOutlined, DeleteOutlined, SyncOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getWatchList, addToWatch, removeFromWatch, triggerUpdate,
  type WatchListItem as WatchItem,
} from '../lib/api';

const { Title, Text } = Typography;

const statusMap: Record<string, { color: string; label: string }> = {
  normal: { color: 'green', label: '正常' },
  warning: { color: 'orange', label: '预警' },
  critical: { color: 'red', label: '严重' },
};

const recColor: Record<string, string> = {
  '买入(强烈)': 'red',
  '买入(谨慎)': 'volcano',
  '持有': 'orange',
  '观望': 'blue',
  '卖出': 'green',
};

const WatchListPage: React.FC = () => {
  const queryClient = useQueryClient();
  const [addModalOpen, setAddModalOpen] = useState(false);
  const [newCode, setNewCode] = useState('');
  const [newName, setNewName] = useState('');

  const { data: watchData, isLoading } = useQuery({
    queryKey: ['watch'],
    queryFn: getWatchList,
  });

  const addMutation = useMutation({
    mutationFn: () => addToWatch(newCode.trim(), newName.trim()),
    onSuccess: (resp) => {
      message.success(resp.message);
      setAddModalOpen(false);
      setNewCode('');
      setNewName('');
      queryClient.invalidateQueries({ queryKey: ['watch'] });
    },
    onError: () => message.error('添加失败'),
  });

  const removeMutation = useMutation({
    mutationFn: removeFromWatch,
    onSuccess: () => {
      message.success('已移除');
      queryClient.invalidateQueries({ queryKey: ['watch'] });
    },
  });

  const updateMutation = useMutation({
    mutationFn: triggerUpdate,
    onSuccess: (resp) => {
      message.success(resp.message || '更新已触发');
      queryClient.invalidateQueries({ queryKey: ['watch'] });
    },
  });

  const items = watchData?.items ?? [];

  const columns = [
    {
      title: '代码',
      dataIndex: 'stock_code',
      key: 'code',
      width: 100,
      render: (code: string) => (
        <Text strong style={{ color: 'var(--color-text-primary)', fontFamily: 'monospace' }}>
          {code}
        </Text>
      ),
    },
    {
      title: '名称',
      dataIndex: 'stock_name',
      key: 'name',
      width: 120,
      render: (name: string) => (
        <Text style={{ color: 'var(--color-text-primary)' }}>{name || '-'}</Text>
      ),
    },
    {
      title: '建议',
      dataIndex: 'recommendation',
      key: 'rec',
      width: 120,
      render: (rec: string) =>
        rec ? <Tag color={recColor[rec] || 'default'}>{rec}</Tag> : <Text style={{ color: 'var(--color-text-muted)' }}>-</Text>,
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 80,
      render: (s: string) => {
        const cfg = statusMap[s] || { color: 'default', label: s };
        return <Tag color={cfg.color}>{cfg.label}</Tag>;
      },
    },
    {
      title: '上次更新',
      dataIndex: 'last_updated_at',
      key: 'updated',
      width: 140,
      render: (d: string | null) => (
        <Text style={{ color: 'var(--color-text-muted)', fontSize: 13 }}>
          {d ? new Date(d).toLocaleDateString('zh-CN') : '-'}
        </Text>
      ),
    },
    {
      title: '操作',
      key: 'action',
      width: 180,
      render: (_: unknown, record: WatchItem) => (
        <Space size="small">
          <Button
            size="small"
            icon={<SyncOutlined />}
            onClick={() => updateMutation.mutate(record.stock_code)}
            loading={updateMutation.isPending}
          >
            更新
          </Button>
          <Popconfirm
            title={`确定移除 ${record.stock_code}?`}
            onConfirm={() => removeMutation.mutate(record.stock_code)}
          >
            <Button size="small" danger icon={<DeleteOutlined />}>
              移除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div className="max-w-6xl mx-auto space-y-4">
      <div className="flex items-center justify-between">
        <Title level={4} style={{ color: 'var(--color-text-primary)', margin: 0 }}>
          监控列表
          <Text style={{ color: 'var(--color-text-muted)', fontSize: 14 }} className="ml-2">
            {items.length} 个标的
          </Text>
        </Title>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setAddModalOpen(true)}>
          添加标的
        </Button>
      </div>

      <Card style={{ background: 'var(--color-bg-secondary)' }}>
        <Table
          dataSource={items}
          columns={columns}
          rowKey="stock_code"
          loading={isLoading}
          pagination={false}
          size="middle"
          locale={{ emptyText: '暂无监控标的，点击"添加标的"开始' }}
        />
      </Card>

      {/* 添加Modal */}
      <Modal
        title="添加监控标的"
        open={addModalOpen}
        onOk={() => addMutation.mutate()}
        onCancel={() => setAddModalOpen(false)}
        confirmLoading={addMutation.isPending}
        okText="添加"
        cancelText="取消"
      >
        <div className="space-y-3 py-2">
          <div>
            <Text style={{ color: 'var(--color-text-secondary)' }}>股票代码</Text>
            <Input
              placeholder="如 300358"
              value={newCode}
              onChange={(e) => setNewCode(e.target.value)}
              className="mt-1"
            />
          </div>
          <div>
            <Text style={{ color: 'var(--color-text-secondary)' }}>股票名称（可选）</Text>
            <Input
              placeholder="如 湖南裕能"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              className="mt-1"
            />
          </div>
        </div>
      </Modal>
    </div>
  );
};

export default WatchListPage;
