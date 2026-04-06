/**
 * 归档与知识库页 - 归档资料浏览、搜索、知识库统计
 */
import { useState, useMemo, useRef, useCallback } from 'react';
import {
  Card,
  Table,
  Tag,
  Button,
  Input,
  Select,
  Space,
  Typography,
  Spin,
  Modal,
  message,
  Statistic,
  Empty,
  Drawer,
  List,
} from 'antd';
import {
  SearchOutlined,
  ReloadOutlined,
  DeleteOutlined,
  FileTextOutlined,
  DatabaseOutlined,
  InboxOutlined,
  EyeOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  listArchives,
  getArchiveContent,
  deleteArchive,
  getArchiveStats,
  getKnowledgeStats,
  searchKnowledgeIntel,
  rebuildKnowledge,
  type IntelArchive,
} from '../lib/api';

const { Title, Text, Paragraph } = Typography;

const STATUS_COLORS = {
  success: '#22c55e',
  error: '#ef4444',
  warning: '#f59e0b',
  purple: '#8b5cf6',
} as const;

const categoryLabel: Record<string, string> = {
  stock_info: '基本信息',
  daily_prices: '日行情',
  weekly_prices: '周行情',
  monthly_prices: '月行情',
  financial_statements: '财务报表',
  financial_indicators: '财务指标',
  valuation: '估值数据',
  industry_info: '行业信息',
};

function IntelArchives() {
  const queryClient = useQueryClient();
  const [keyword, setKeyword] = useState('');
  const [appliedKeyword, setAppliedKeyword] = useState('');
  const [filterCategory, setFilterCategory] = useState<string | undefined>();
  const [filterCode, setFilterCode] = useState<string | undefined>();
  const [page, setPage] = useState(1);
  const [contentDrawerOpen, setContentDrawerOpen] = useState(false);
  const [selectedArchive, setSelectedArchive] = useState<{ id: number; title: string } | null>(null);
  const [knowledgeSearch, setKnowledgeSearch] = useState('');

  const debounceTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const { data: archivesData, isLoading } = useQuery({
    queryKey: ['intel-archives', appliedKeyword, filterCategory, filterCode, page],
    queryFn: () =>
      listArchives({
        keyword: appliedKeyword || undefined,
        category: filterCategory,
        stock_code: filterCode,
        page,
        page_size: 20,
      }),
  });

  const { data: stats = {} } = useQuery({
    queryKey: ['archive-stats'],
    queryFn: getArchiveStats,
  });

  const { data: knowledgeStats } = useQuery({
    queryKey: ['knowledge-stats'],
    queryFn: getKnowledgeStats,
  });

  const { data: archiveContent, isLoading: contentLoading } = useQuery({
    queryKey: ['archive-content', selectedArchive?.id],
    queryFn: () => {
      if (!selectedArchive) throw new Error('No archive selected');
      return getArchiveContent(selectedArchive.id);
    },
    enabled: selectedArchive !== null,
  });

  const deleteMutation = useMutation({
    mutationFn: deleteArchive,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['intel-archives'] });
      queryClient.invalidateQueries({ queryKey: ['archive-stats'] });
      message.success('归档已删除');
    },
    onError: (err: unknown) => {
      message.error('删除失败');
      console.error('[IntelArchives] delete failed:', err);
    },
  });

  const rebuildMutation = useMutation({
    mutationFn: rebuildKnowledge,
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ['knowledge-stats'] });
      queryClient.invalidateQueries({ queryKey: ['archive-stats'] });
      message.success(`索引重建完成: ${result.indexed_count}/${result.total_count}`);
    },
    onError: (err: unknown) => {
      message.error('重建失败');
      console.error('[IntelArchives] rebuild failed:', err);
    },
  });

  const knowledgeSearchMutation = useMutation({
    mutationFn: searchKnowledgeIntel,
    onSuccess: () => {},
    onError: (err: unknown) => {
      message.error('搜索失败');
      console.error('[IntelArchives] knowledge search failed:', err);
    },
  });

  const handleKeywordChange = useCallback((value: string) => {
    setKeyword(value);
    setPage(1);
    if (debounceTimer.current) clearTimeout(debounceTimer.current);
    debounceTimer.current = setTimeout(() => setAppliedKeyword(value), 300);
  }, []);

  const handleKnowledgeSearch = useCallback(() => {
    if (!knowledgeSearch.trim()) return;
    knowledgeSearchMutation.mutate({ keyword: knowledgeSearch, page: 1, page_size: 20 });
  }, [knowledgeSearch, knowledgeSearchMutation]);

  const handleViewContent = useCallback((record: IntelArchive) => {
    setSelectedArchive({ id: record.id, title: record.title });
    setContentDrawerOpen(true);
  }, []);

  const handleDelete = useCallback((record: IntelArchive) => {
    Modal.confirm({
      title: `确认删除归档 "${record.title}"?`,
      okText: '删除',
      cancelText: '取消',
      okButtonProps: { danger: true },
      onOk: () => deleteMutation.mutateAsync(record.id),
    });
  }, [deleteMutation]);

  const columns = useMemo(() => [
    {
      title: '标题',
      dataIndex: 'title',
      key: 'title',
      ellipsis: true,
      render: (title: string) => (
        <Text style={{ color: 'var(--color-text-primary)' }}>{title}</Text>
      ),
    },
    {
      title: '股票',
      dataIndex: 'stock_code',
      key: 'stock_code',
      width: 90,
      render: (code: string) => <Tag>{code}</Tag>,
    },
    {
      title: '类别',
      dataIndex: 'category',
      key: 'category',
      width: 110,
      render: (cat: string) => (
        <Tag color="blue">{categoryLabel[cat] || cat}</Tag>
      ),
    },
    {
      title: '来源',
      dataIndex: 'source_name',
      key: 'source_name',
      width: 90,
    },
    {
      title: '已索引',
      dataIndex: 'indexed',
      key: 'indexed',
      width: 70,
      render: (indexed: boolean) => (
        indexed ? <Tag color="success">是</Tag> : <Tag>否</Tag>
      ),
    },
    {
      title: '归档时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 160,
      render: (d: string) => (
        <Text style={{ color: 'var(--color-text-muted)', fontSize: 12 }}>{d}</Text>
      ),
    },
    {
      title: '操作',
      key: 'actions',
      width: 130,
      render: (_: unknown, record: IntelArchive) => (
        <Space size="small">
          <Button size="small" icon={<EyeOutlined />} onClick={() => handleViewContent(record)}>
            查看
          </Button>
          <Button size="small" danger icon={<DeleteOutlined />} onClick={() => handleDelete(record)} />
        </Space>
      ),
    },
  ], [handleViewContent, handleDelete]);

  const totalArchives = archivesData?.total ?? 0;
  const collectionCount = knowledgeStats?.vector_collections?.length ?? 0;
  const indexedCount = typeof stats.indexed === 'number' ? stats.indexed : 0;
  const totalCount = typeof stats.total === 'number' ? stats.total : 0;
  const pendingCount = totalCount - indexedCount;
  const knowledgeResults = knowledgeSearchMutation.data ?? null;

  return (
    <div className="space-y-6 max-w-7xl">
      <div className="flex items-center justify-between">
        <div>
          <Title level={4} style={{ color: 'var(--color-text-primary)', margin: 0 }}>
            归档与知识库
          </Title>
          <Text style={{ color: 'var(--color-text-secondary)' }}>
            浏览已归档的数据资料，搜索知识库
          </Text>
        </div>
        <Button
          icon={<ReloadOutlined />}
          onClick={() => rebuildMutation.mutate()}
          loading={rebuildMutation.isPending}
        >
          重建索引
        </Button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card size="small">
          <Statistic
            title={<Text style={{ color: 'var(--color-text-secondary)', fontSize: 12 }}>归档总数</Text>}
            value={totalArchives}
            prefix={<InboxOutlined style={{ color: 'var(--color-brand)' }} />}
            valueStyle={{ color: 'var(--color-text-primary)' }}
          />
        </Card>
        <Card size="small">
          <Statistic
            title={<Text style={{ color: 'var(--color-text-secondary)', fontSize: 12 }}>向量集合</Text>}
            value={collectionCount}
            prefix={<DatabaseOutlined style={{ color: STATUS_COLORS.purple }} />}
            valueStyle={{ color: 'var(--color-text-primary)' }}
          />
        </Card>
        <Card size="small">
          <Statistic
            title={<Text style={{ color: 'var(--color-text-secondary)', fontSize: 12 }}>已索引</Text>}
            value={indexedCount}
            prefix={<FileTextOutlined style={{ color: STATUS_COLORS.success }} />}
            valueStyle={{ color: 'var(--color-text-primary)' }}
          />
        </Card>
        <Card size="small">
          <Statistic
            title={<Text style={{ color: 'var(--color-text-secondary)', fontSize: 12 }}>待索引</Text>}
            value={pendingCount}
            prefix={<FileTextOutlined style={{ color: STATUS_COLORS.warning }} />}
            valueStyle={{ color: 'var(--color-text-primary)' }}
          />
        </Card>
      </div>

      <Card size="small">
        <Space wrap>
          <Input
            placeholder="搜索关键词"
            prefix={<SearchOutlined />}
            value={keyword}
            onChange={(e) => handleKeywordChange(e.target.value)}
            style={{ width: 200 }}
            allowClear
          />
          <Input
            placeholder="股票代码"
            value={filterCode || ''}
            onChange={(e) => { setFilterCode(e.target.value || undefined); setPage(1); }}
            style={{ width: 140 }}
            allowClear
          />
          <Select
            placeholder="数据类别"
            value={filterCategory}
            onChange={(v) => { setFilterCategory(v); setPage(1); }}
            style={{ width: 140 }}
            allowClear
            options={Object.entries(categoryLabel).map(([k, v]) => ({ label: v, value: k }))}
          />
        </Space>
      </Card>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spin /></div>
        ) : !archivesData?.items?.length ? (
          <Empty description="暂无归档资料" />
        ) : (
          <Table
            dataSource={archivesData.items}
            columns={columns}
            rowKey="id"
            pagination={{
              current: page,
              total: archivesData.total,
              pageSize: 20,
              onChange: setPage,
              showTotal: (total) => `共 ${total} 条`,
              size: 'small',
            }}
            size="middle"
          />
        )}
      </Card>

      <Card title={
        <span>
          <SearchOutlined style={{ marginRight: 8 }} />
          知识库语义检索
        </span>
      }>
        <Space.Compact className="w-full mb-4">
          <Input
            placeholder="输入自然语言查询，如「估值分析」「财务风险评估」"
            size="large"
            value={knowledgeSearch}
            onChange={(e) => setKnowledgeSearch(e.target.value)}
            onPressEnter={handleKnowledgeSearch}
          />
          <Button
            type="primary"
            size="large"
            onClick={handleKnowledgeSearch}
            loading={knowledgeSearchMutation.isPending}
          >
            语义搜索
          </Button>
        </Space.Compact>

        {knowledgeSearchMutation.isError && (
          <Text style={{ color: STATUS_COLORS.error }} className="block mb-4">
            搜索失败，请重试
          </Text>
        )}

        {knowledgeResults && (
          knowledgeResults.items.length === 0 ? (
            <Empty description="未找到匹配的知识" />
          ) : (
            <List
              dataSource={knowledgeResults.items}
              renderItem={(item: IntelArchive) => (
                <List.Item
                  actions={[
                    <Button key="view" size="small" type="link" onClick={() => handleViewContent(item)}>
                      查看详情
                    </Button>,
                  ]}
                >
                  <List.Item.Meta
                    title={
                      <div className="flex items-center gap-2">
                        <Text style={{ color: 'var(--color-text-primary)' }}>{item.title}</Text>
                        <Tag>{item.stock_code}</Tag>
                        <Tag color="blue">{categoryLabel[item.category] || item.category}</Tag>
                      </div>
                    }
                    description={
                      <Paragraph
                        ellipsis={{ rows: 2 }}
                        style={{ color: 'var(--color-text-secondary)', marginBottom: 0, fontSize: 13 }}
                      >
                        {item.summary}
                      </Paragraph>
                    }
                  />
                </List.Item>
              )}
            />
          )
        )}
      </Card>

      <Drawer
        title={selectedArchive?.title || '归档详情'}
        open={contentDrawerOpen}
        onClose={() => { setContentDrawerOpen(false); setSelectedArchive(null); }}
        width={640}
      >
        {contentLoading ? (
          <div className="flex justify-center py-12"><Spin /></div>
        ) : archiveContent ? (
          <pre
            style={{
              background: 'var(--color-bg-secondary)',
              padding: 16,
              borderRadius: 8,
              overflow: 'auto',
              maxHeight: '80vh',
              fontSize: 13,
              color: 'var(--color-text-primary)',
            }}
          >
            {JSON.stringify(archiveContent.content, null, 2)}
          </pre>
        ) : (
          <Empty description="无法加载内容" />
        )}
      </Drawer>
    </div>
  );
}

export default IntelArchives;
