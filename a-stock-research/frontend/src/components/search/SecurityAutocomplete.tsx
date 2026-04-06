import React, { useEffect, useState } from 'react';
import { SearchOutlined } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { Input, Spin } from 'antd';
import { searchSecurities, type SecurityLookupItem } from '../../lib/api';

interface SecurityAutocompleteProps {
  value: string;
  selectedSecurity: SecurityLookupItem | null;
  onValueChange: (value: string) => void;
  onSelectedSecurityChange: (security: SecurityLookupItem | null) => void;
  onSubmit: (security?: SecurityLookupItem) => void;
  placeholder?: string;
  maxWidth?: number;
}

export function formatSecurityDisplay(security: SecurityLookupItem): string {
  if (!security.stock_name) {
    return security.stock_code;
  }
  return `${security.stock_name} · ${security.stock_code}`;
}

export function resolveTypedSecurityCode(value: string): string {
  const trimmed = value.trim().toUpperCase();
  const match = trimmed.match(/(\d{6})/);
  return match?.[1] ?? '';
}

const cycleIndex = (current: number, size: number, step: number) => {
  if (size <= 0) return 0;
  return (current + step + size) % size;
};

const SecurityAutocomplete: React.FC<SecurityAutocompleteProps> = ({
  value,
  selectedSecurity,
  onValueChange,
  onSelectedSecurityChange,
  onSubmit,
  placeholder = '输入股票代码或名称',
  maxWidth = 360,
}) => {
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);

  const selectedDisplay = selectedSecurity ? formatSecurityDisplay(selectedSecurity) : '';
  const query = value.trim();
  const shouldSearch = query.length > 0 && query !== selectedDisplay;

  const { data, isFetching } = useQuery({
    queryKey: ['security-lookup', query],
    queryFn: () => searchSecurities(query),
    enabled: shouldSearch,
    staleTime: 5 * 60 * 1000,
    placeholderData: (previous) => previous,
  });

  const items = shouldSearch ? data?.items ?? [] : [];

  useEffect(() => {
    setActiveIndex(0);
  }, [query, items.length]);

  const showDropdown = open && shouldSearch && (isFetching || items.length > 0 || query.length > 0);

  const commitSelection = (security: SecurityLookupItem) => {
    onSelectedSecurityChange(security);
    onValueChange(formatSecurityDisplay(security));
    setOpen(false);
    setActiveIndex(0);
  };

  const handleChange = (nextValue: string) => {
    if (selectedSecurity && nextValue !== selectedDisplay) {
      onSelectedSecurityChange(null);
    }
    onValueChange(nextValue);
    setOpen(true);
  };

  return (
    <div className="relative" style={{ maxWidth }}>
      <Input
        size="large"
        value={value}
        placeholder={placeholder}
        prefix={<SearchOutlined />}
        suffix={isFetching ? <Spin size="small" /> : null}
        onChange={(event) => handleChange(event.target.value)}
        onFocus={() => {
          if (shouldSearch) {
            setOpen(true);
          }
        }}
        onBlur={() => {
          window.setTimeout(() => setOpen(false), 100);
        }}
        onKeyDown={(event) => {
          if (event.key === 'Tab' && items.length > 0) {
            event.preventDefault();
            setOpen(true);
            setActiveIndex((current) => cycleIndex(current, items.length, event.shiftKey ? -1 : 1));
            return;
          }

          if (event.key === 'ArrowDown' && items.length > 0) {
            event.preventDefault();
            setOpen(true);
            setActiveIndex((current) => cycleIndex(current, items.length, 1));
            return;
          }

          if (event.key === 'ArrowUp' && items.length > 0) {
            event.preventDefault();
            setOpen(true);
            setActiveIndex((current) => cycleIndex(current, items.length, -1));
            return;
          }

          if (event.key === 'Enter') {
            if (showDropdown && items[activeIndex]) {
              event.preventDefault();
              const nextSecurity = items[activeIndex];
              commitSelection(nextSecurity);
              onSubmit(nextSecurity);
              return;
            }

            onSubmit();
            return;
          }

          if (event.key === 'Escape') {
            setOpen(false);
          }
        }}
        style={{ width: '100%' }}
      />

      {showDropdown && (
        <div
          role="listbox"
          className="absolute left-0 right-0 mt-2 overflow-hidden rounded-2xl"
          style={{
            background: 'var(--color-bg-secondary)',
            border: '1px solid var(--color-border)',
            boxShadow: '0 18px 40px rgba(15, 23, 42, 0.22)',
            zIndex: 30,
          }}
        >
          <div className="max-h-80 overflow-y-auto py-1">
            {items.length > 0 ? (
              items.map((item, index) => {
                const active = index === activeIndex;

                return (
                  <button
                    key={`${item.stock_code}-${item.stock_name}`}
                    type="button"
                    role="option"
                    aria-selected={active}
                    className="w-full px-4 py-3 text-left transition-colors"
                    style={{
                      background: active ? 'rgba(59, 130, 246, 0.14)' : 'transparent',
                      border: 'none',
                      cursor: 'pointer',
                    }}
                    onMouseEnter={() => setActiveIndex(index)}
                    onMouseDown={(event) => {
                      event.preventDefault();
                      commitSelection(item);
                    }}
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <div className="flex items-center gap-2 flex-wrap">
                          <span
                            className="font-medium"
                            style={{ color: 'var(--color-text-primary)' }}
                          >
                            {item.stock_name || item.stock_code}
                          </span>
                          <span
                            className="text-sm"
                            style={{ color: 'var(--color-brand)', fontFamily: 'ui-monospace, SFMono-Regular, monospace' }}
                          >
                            {item.stock_code}
                          </span>
                        </div>
                        <div
                          className="mt-1 text-xs"
                          style={{ color: 'var(--color-text-secondary)' }}
                        >
                          {item.exchange || 'A股标的'}
                        </div>
                      </div>

                      <div className="flex flex-wrap items-center justify-end gap-2">
                        {item.has_report && (
                          <span
                            className="rounded-full px-2 py-1 text-[11px]"
                            style={{
                              background: 'rgba(16, 185, 129, 0.14)',
                              color: 'var(--color-success)',
                            }}
                          >
                            已研究
                          </span>
                        )}
                        {item.in_watchlist && (
                          <span
                            className="rounded-full px-2 py-1 text-[11px]"
                            style={{
                              background: 'rgba(245, 158, 11, 0.14)',
                              color: 'var(--color-warning)',
                            }}
                          >
                            自选
                          </span>
                        )}
                      </div>
                    </div>
                  </button>
                );
              })
            ) : (
              <div className="px-4 py-4 text-sm" style={{ color: 'var(--color-text-secondary)' }}>
                没有找到匹配标的
              </div>
            )}
          </div>

          <div
            className="flex items-center justify-between px-4 py-2 text-xs"
            style={{
              borderTop: '1px solid var(--color-border)',
              color: 'var(--color-text-muted)',
            }}
          >
            <span>支持代码 / 名称 / 拼音首字母</span>
            <span>Tab/方向键切换，Enter开始研究</span>
          </div>
        </div>
      )}
    </div>
  );
};

export default SecurityAutocomplete;
