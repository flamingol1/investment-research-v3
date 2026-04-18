/**
 * 顶栏 - 搜索 + 主题切换
 */
import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  SearchOutlined,
  BulbOutlined,
  BulbFilled,
} from '@ant-design/icons';
import { Input, Tooltip } from 'antd';
import { useAppStore } from '../../lib/store';

const Header: React.FC = () => {
  const navigate = useNavigate();
  const { sidebarCollapsed, toggleSidebar, theme, setTheme } = useAppStore();
  const [searchValue, setSearchValue] = useState('');

  const handleSearch = (value: string) => {
    const trimmed = value.trim();
    if (!trimmed) return;
    if (/^\d{6}$/.test(trimmed)) {
      navigate(`/research?stock=${trimmed}`);
    } else {
      navigate(`/search?q=${encodeURIComponent(trimmed)}`);
    }
    setSearchValue('');
  };

  return (
    <header
      className="h-14 flex items-center justify-between px-4 border-b"
      style={{
        background: 'var(--color-bg-secondary)',
        borderColor: 'var(--color-border)',
      }}
    >
      <div className="flex items-center gap-3">
        <button
          onClick={toggleSidebar}
          className="text-lg p-1 rounded transition-colors"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {sidebarCollapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
        </button>

        <Input
          placeholder="输入股票代码或搜索关键词..."
          prefix={<SearchOutlined style={{ color: 'var(--color-text-muted)' }} />}
          value={searchValue}
          onChange={(e) => setSearchValue(e.target.value)}
          onPressEnter={() => handleSearch(searchValue)}
          style={{ width: 360, background: 'var(--color-bg-elevated)' }}
          allowClear
        />
      </div>

      <div className="flex items-center gap-2">
        <Tooltip title={theme === 'dark' ? '切换亮色' : '切换暗色'}>
          <button
            onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
            className="text-lg p-2 rounded-lg transition-colors"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {theme === 'dark' ? <BulbOutlined /> : <BulbFilled />}
          </button>
        </Tooltip>
      </div>
    </header>
  );
};

export default Header;
