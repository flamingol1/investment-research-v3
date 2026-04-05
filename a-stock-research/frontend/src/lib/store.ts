/**
 * Zustand 状态管理 - 客户端UI状态
 */
import { create } from 'zustand';

interface AppState {
  // 侧边栏
  sidebarCollapsed: boolean;
  toggleSidebar: () => void;
  setSidebarCollapsed: (v: boolean) => void;

  // 主题
  theme: 'dark' | 'light';
  setTheme: (theme: 'dark' | 'light') => void;

  // 当前研究任务
  activeTaskId: string | null;
  setActiveTask: (taskId: string | null) => void;
}

export const useAppStore = create<AppState>((set) => ({
  sidebarCollapsed: false,
  toggleSidebar: () => set((s) => ({ sidebarCollapsed: !s.sidebarCollapsed })),
  setSidebarCollapsed: (v) => set({ sidebarCollapsed: v }),

  theme: (localStorage.getItem('theme') as 'dark' | 'light') || 'dark',
  setTheme: (theme) => {
    localStorage.setItem('theme', theme);
    const root = document.documentElement;
    root.classList.toggle('dark', theme === 'dark');
    root.classList.toggle('light', theme === 'light');
    set({ theme });
  },

  activeTaskId: null,
  setActiveTask: (taskId) => set({ activeTaskId: taskId }),
}));
