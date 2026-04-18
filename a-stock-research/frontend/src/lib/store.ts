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

  theme: (localStorage.getItem('theme') as 'dark' | 'light') || 'light',
  setTheme: (theme) => {
    localStorage.setItem('theme', theme);
    const root = document.documentElement;
    if (theme === 'dark') {
      root.classList.add('dark');
    } else {
      root.classList.remove('dark');
    }
    set({ theme });
  },

  activeTaskId: null,
  setActiveTask: (taskId) => set({ activeTaskId: taskId }),
}));
