/**
 * 知识库语义搜索页面。
 */
import React, { useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Empty,
  Input,
  List,
  Select,
  Space,
  Spin,
  Tag,
  Typography,
} from 'antd';
import { SearchOutlined } from '@ant-design/icons';
import { useMutation } from '@tanstack/react-query';
import { searchKnowledge, type SearchItem } from '../lib/api';

const { Title, Text, Paragraph } = Typography;

const categoryOptions = [
  { value: '', label: '全部分类' },
  { value: 'stock', label: '个股研究' },
  { value: 'industry', label: '行业分析' },
  { value: 'macro', label: '宏观环境' },
  { value: 'report', label: '研究报告' },
  { value: 'risk', label: '风险分析' },
  { value: 'decision', label: '投资决策' },
];

const categoryColor: Record<string, string> = {
  stock: 'blue',
  industry: 'purple',
  macro: 'cyan',
  report: 'geekblue',
  risk: 'red',
  decision: 'gold',
};

const SearchPage: React.FC = () => {
  const [query, setQuery] = useState('');
  const [category, setCategory] = useState('');
  const [results, setResults] = useState<SearchItem[]>([]);
  const [warning, setWarning] = useState('');

  const searchMutation = useMutation({
    mutationFn: () => searchKnowledge({ query, category: category || undefined, num_results: 10 }),
    onSuccess: (data) => {
      setResults(data.results);
      setWarning(data.warning ?? '');
    },
    onError: () => {
      setResults([]);
      setWarning('搜索请求失败，请稍后重试。');
    },
  });

  const handleSearch = () => {
    if (!query.trim()) return;
    setWarning('');
    searchMutation.mutate();
  };

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <Title level={4} style={{ color: 'var(--color-text-primary)' }}>
        知识库语义搜索
      </Title>

      <Card style={{ background: 'var(--color-bg-secondary)' }}>
        <Space.Compact className="w-full">
          <Input
            size="large"
            placeholder="输入搜索关键词，如“估值分析”“护城河”或“行业景气度”"
            prefix={<SearchOutlined />}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onPressEnter={handleSearch}
          />
          <Select
            size="large"
            value={category}
            onChange={setCategory}
            options={categoryOptions}
            style={{ width: 140 }}
          />
          <Button
            size="large"
            type="primary"
            onClick={handleSearch}
            loading={searchMutation.isPending}
          >
            搜索
          </Button>
        </Space.Compact>
      </Card>

      {warning && (
        <Alert
          showIcon
          type="warning"
          message={warning}
        />
      )}

      {searchMutation.isPending ? (
        <div className="flex justify-center py-12">
          <Spin size="large" />
        </div>
      ) : results.length === 0 && (searchMutation.isSuccess || searchMutation.isError) ? (
        <Card>
          <Empty description={`未找到与“${query}”相关的内容`} />
        </Card>
      ) : (
        <List
          dataSource={results}
          renderItem={(item) => {
            const similarityPct = Math.max(0, Math.min(100, Math.round(item.similarity * 100)));

            return (
              <List.Item
                style={{
                  background: 'var(--color-bg-secondary)',
                  borderBottom: '1px solid var(--color-border)',
                  padding: '16px 20px',
                }}
              >
                <div className="w-full space-y-2">
                  <div className="flex items-center gap-2">
                    <Text strong style={{ color: 'var(--color-text-primary)', fontFamily: 'monospace' }}>
                      {item.stock_code}
                    </Text>
                    <Text style={{ color: 'var(--color-text-secondary)' }}>
                      {item.stock_name}
                    </Text>
                    <Tag color={categoryColor[item.category] || 'default'}>
                      {item.category}
                    </Tag>
                    <Text style={{ color: 'var(--color-text-muted)', fontSize: 12, marginLeft: 'auto' }}>
                      相似度 {similarityPct}%
                    </Text>
                  </div>
                  <Paragraph
                    ellipsis={{ rows: 3 }}
                    style={{
                      color: 'var(--color-text-secondary)',
                      fontSize: 13,
                      lineHeight: 1.8,
                      marginBottom: 0,
                    }}
                  >
                    {item.document}
                  </Paragraph>
                  {item.date && (
                    <Text style={{ color: 'var(--color-text-muted)', fontSize: 11 }}>
                      {item.date}
                    </Text>
                  )}
                </div>
              </List.Item>
            );
          }}
        />
      )}
    </div>
  );
};

export default SearchPage;
