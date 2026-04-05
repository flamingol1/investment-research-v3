/**
 * 知识库语义搜索页面
 */
import React, { useState } from 'react';
import {
  Card, Input, Select, List, Tag, Typography, Empty, Spin, Button, Space,
} from 'antd';
import {
  SearchOutlined,
} from '@ant-design/icons';
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

  const searchMutation = useMutation({
    mutationFn: () => searchKnowledge({ query, category: category || undefined, num_results: 10 }),
    onSuccess: (data) => {
      setResults(data.results);
    },
  });

  const handleSearch = () => {
    if (!query.trim()) return;
    searchMutation.mutate();
  };

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <Title level={4} style={{ color: 'var(--color-text-primary)' }}>
        知识库语义搜索
      </Title>

      {/* 搜索栏 */}
      <Card style={{ background: 'var(--color-bg-secondary)' }}>
        <Space.Compact className="w-full">
          <Input
            size="large"
            placeholder="输入搜索关键词，如「估值分析」「新能源」「护城河评估」..."
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

      {/* 结果 */}
      {searchMutation.isPending ? (
        <div className="flex justify-center py-12">
          <Spin size="large" />
        </div>
      ) : results.length === 0 && searchMutation.isSuccess ? (
        <Card>
          <Empty description={`未找到与"${query}"相关的内容`} />
        </Card>
      ) : (
        <List
          dataSource={results}
          renderItem={(item) => (
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
                    相似度: {(item.similarity * 100).toFixed(0)}%
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
          )}
        />
      )}
    </div>
  );
};

export default SearchPage;
