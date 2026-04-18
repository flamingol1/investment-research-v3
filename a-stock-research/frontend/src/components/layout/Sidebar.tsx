/**
 * 侧边栏导航
 */
import React from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import {
  DashboardOutlined,
  SearchOutlined,
  FundOutlined,
  HistoryOutlined,
  EyeOutlined,
  BulbOutlined,
  DatabaseOutlined,
} from '@ant-design/icons';
import { useAppStore } from '../../lib/store';

const menuItems = [
  { key: '/', icon: <DashboardOutlined />, label: '仪表盘' },
  { key: '/research', icon: <SearchOutlined />, label: '股票研究' },
  { key: '/watch', icon: <EyeOutlined />, label: '监控列表' },
  { key: '/history', icon: <HistoryOutlined />, label: '研究历史' },
  { key: '/search', icon: <FundOutlined />, label: '知识库搜索' },
];

const intelItems = [
  { key: '/intel/sources', label: '数据源管理' },
  { key: '/intel/collect', label: '采集任务' },
  { key: '/intel/archives', label: '归档与知识库' },
];

const Sidebar: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const collapsed = useAppStore((s) => s.sidebarCollapsed);

  return (
    <aside
      className={`h-screen flex flex-col border-r transition-all duration-200 ${
        collapsed ? 'w-16' : 'w-60'
      }`}
      style={{
        background: 'var(--color-bg-secondary)',
        borderColor: 'var(--color-border)',
      }}
    >
      {/* Logo */}
      <div
        className="flex items-center gap-3 px-4 border-b h-14 cursor-pointer"
        style={{ borderColor: 'var(--color-border)' }}
        onClick={() => navigate('/')}
      >
        <div
          className="flex items-center justify-center w-8 h-8 rounded-lg text-white font-bold text-sm"
          style={{ background: 'var(--color-brand)' }}
        >
          A
        </div>
        {!collapsed && (
          <div className="flex flex-col">
            <span className="text-sm font-semibold" style={{ color: 'var(--color-text-primary)' }}>
              A股投研系统
            </span>
            <span className="text-xs" style={{ color: 'var(--color-text-muted)' }}>
              Multi-Agent Research
            </span>
          </div>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 py-3 px-2 space-y-1 overflow-y-auto">
        {menuItems.map((item) => {
          const isActive = location.pathname === item.key ||
            (item.key !== '/' && location.pathname.startsWith(item.key));
          return (
            <button
              key={item.key}
              onClick={() => navigate(item.key)}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-colors ${
                isActive
                  ? 'font-medium'
                  : ''
              }`}
              style={isActive
                ? { background: 'var(--color-brand)', color: 'var(--color-nav-active-text)' }
                : { color: 'var(--color-nav-inactive-text)' }
              }
              onMouseEnter={(e) => {
                if (!isActive) {
                  e.currentTarget.style.background = 'var(--color-nav-hover-bg)';
                  e.currentTarget.style.color = 'var(--color-text-primary)';
                }
              }}
              onMouseLeave={(e) => {
                if (!isActive) {
                  e.currentTarget.style.background = 'transparent';
                  e.currentTarget.style.color = 'var(--color-nav-inactive-text)';
                }
              }}
              title={collapsed ? item.label : undefined}
            >
              <span className="text-lg">{item.icon}</span>
              {!collapsed && <span>{item.label}</span>}
            </button>
          );
        })}

        {/* Intelligence Hub Section */}
        {!collapsed && (
          <div className="pt-4 pb-1 px-3">
            <span
              className="text-xs font-semibold uppercase tracking-wider"
              style={{ color: 'var(--color-section-label)' }}
            >
              情报中心
            </span>
          </div>
        )}
        {collapsed && <div className="my-2 border-t" style={{ borderColor: 'var(--color-border)' }} />}
        {intelItems.map((item) => {
          const isActive = location.pathname === item.key;
          return (
            <button
              key={item.key}
              onClick={() => navigate(item.key)}
              className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors ${
                isActive ? 'font-medium' : ''
              }`}
              style={isActive
                ? { background: 'var(--color-brand)', color: 'var(--color-nav-active-text)' }
                : { color: 'var(--color-nav-inactive-text)' }
              }
              onMouseEnter={(e) => {
                if (!isActive) {
                  e.currentTarget.style.background = 'var(--color-nav-hover-bg)';
                  e.currentTarget.style.color = 'var(--color-text-primary)';
                }
              }}
              onMouseLeave={(e) => {
                if (!isActive) {
                  e.currentTarget.style.background = 'transparent';
                  e.currentTarget.style.color = 'var(--color-nav-inactive-text)';
                }
              }}
              title={collapsed ? item.label : undefined}
            >
              <span className="text-lg"><DatabaseOutlined /></span>
              {!collapsed && <span>{item.label}</span>}
            </button>
          );
        })}
      </nav>

      {/* Footer */}
      {!collapsed && (
        <div
          className="px-4 py-3 border-t text-xs"
          style={{ borderColor: 'var(--color-border)', color: 'var(--color-footer-text)' }}
        >
          <div className="flex items-center gap-1">
            <BulbOutlined />
            <span>AI驱动 · 多Agent协同</span>
          </div>
        </div>
      )}
    </aside>
  );
};

export default Sidebar;
