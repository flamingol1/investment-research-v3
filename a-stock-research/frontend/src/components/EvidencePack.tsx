/**
 * 证据包展示组件 - 按分类展示研究证据
 */
import React, { useState } from 'react';
import { Tag, Empty } from 'antd';
import { FileTextOutlined, LinkOutlined, TagsOutlined } from '@ant-design/icons';
import type { EvidencePackItem } from '../lib/api';

interface Props {
  evidences: EvidencePackItem[];
}

const categoryColors: Record<string, string> = {
  '行业分析': '#3b82f6',
  '商业模式': '#22c55e',
  '公司治理': '#f59e0b',
  '财务分析': '#8b5cf6',
  '估值分析': '#ef4444',
  '风险分析': '#dc2626',
  '合规事件': '#f97316',
  '专利资料': '#06b6d4',
  '政策资料': '#6366f1',
  '字段质量': '#84cc16',
};

const EvidencePack: React.FC<Props> = ({ evidences }) => {
  const [activeCategory, setActiveCategory] = useState<string>('all');

  if (!evidences.length) {
    return (
      <div className="text-center py-12 rounded-xl" style={{ background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)' }}>
        <Empty description="暂无证据数据" />
      </div>
    );
  }

  const categories = Array.from(new Set(evidences.map((e) => e.category)));
  const filtered = activeCategory === 'all' ? evidences : evidences.filter((e) => e.category === activeCategory);

  return (
    <div>
      {/* 分类筛选 */}
      <div className="flex flex-wrap gap-2 mb-4">
        <button
          onClick={() => setActiveCategory('all')}
          className="px-3 py-1.5 text-xs font-medium rounded-full transition-colors"
          style={{
            background: activeCategory === 'all' ? 'var(--color-brand)' : 'var(--color-bg-elevated)',
            color: activeCategory === 'all' ? '#fff' : 'var(--color-text-secondary)',
            border: 'none',
            cursor: 'pointer',
          }}
        >
          全部 ({evidences.length})
        </button>
        {categories.map((cat) => {
          const count = evidences.filter((e) => e.category === cat).length;
          return (
            <button
              key={cat}
              onClick={() => setActiveCategory(cat)}
              className="px-3 py-1.5 text-xs font-medium rounded-full transition-colors"
              style={{
                background: activeCategory === cat ? (categoryColors[cat] || 'var(--color-brand)') : 'var(--color-bg-elevated)',
                color: activeCategory === cat ? '#fff' : 'var(--color-text-secondary)',
                border: 'none',
                cursor: 'pointer',
              }}
            >
              {cat} ({count})
            </button>
          );
        })}
      </div>

      {/* 证据卡片列表 */}
      <div className="space-y-3">
        {filtered.map((evidence, idx) => (
          <div
            key={idx}
            className="rounded-xl p-4 transition-all hover:shadow-md"
            style={{
              background: 'var(--color-bg-secondary)',
              border: '1px solid var(--color-border)',
            }}
          >
            <div className="flex items-start justify-between gap-3">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-2">
                  <FileTextOutlined style={{ color: categoryColors[evidence.category] || 'var(--color-text-muted)', fontSize: 14 }} />
                  <h4 className="text-sm font-semibold truncate" style={{ color: 'var(--color-text-primary)' }}>
                    {evidence.title}
                  </h4>
                </div>

                {evidence.excerpt && (
                  <p className="text-sm leading-relaxed mb-2" style={{ color: 'var(--color-text-secondary)' }}>
                    {evidence.excerpt}
                  </p>
                )}

                <div className="flex items-center flex-wrap gap-2">
                  <Tag
                    style={{
                      background: `${categoryColors[evidence.category] || '#888'}15`,
                      color: categoryColors[evidence.category] || '#888',
                      border: 'none',
                      fontSize: 11,
                    }}
                  >
                    <TagsOutlined className="mr-1" />
                    {evidence.category}
                  </Tag>

                  {evidence.source && (
                    <span className="text-xs" style={{ color: 'var(--color-text-muted)' }}>
                      来源: {evidence.source}
                    </span>
                  )}

                  {evidence.reference_date && (
                    <span className="text-xs" style={{ color: 'var(--color-text-muted)' }}>
                      {evidence.reference_date}
                    </span>
                  )}

                  {evidence.url && (
                    <a
                      href={evidence.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-xs flex items-center gap-1"
                      style={{ color: 'var(--color-brand)' }}
                    >
                      <LinkOutlined />
                      查看原文
                    </a>
                  )}
                </div>

                {evidence.fields.length > 0 && (
                  <div className="flex flex-wrap gap-1 mt-2">
                    {evidence.fields.map((field, fi) => (
                      <span
                        key={fi}
                        className="text-xs px-2 py-0.5 rounded"
                        style={{
                          background: 'var(--color-bg-elevated)',
                          color: 'var(--color-text-muted)',
                        }}
                      >
                        {field}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default EvidencePack;
