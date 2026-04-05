/**
 * 全局布局 - 侧边栏 + 顶栏 + 内容区
 */
import React from 'react';
import { Outlet } from 'react-router-dom';
import Sidebar from './Sidebar';
import Header from './Header';

const AppLayout: React.FC = () => (
  <div className="flex h-screen overflow-hidden">
    <Sidebar />
    <div className="flex-1 flex flex-col overflow-hidden">
      <Header />
      <main
        className="flex-1 overflow-auto p-6"
        style={{ background: 'var(--color-bg-primary)' }}
      >
        <Outlet />
      </main>
    </div>
  </div>
);

export default AppLayout;
