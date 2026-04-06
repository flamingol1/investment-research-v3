/**
 * 根组件 - 路由配置
 */
import React from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import AppLayout from './components/layout/AppLayout';
import Dashboard from './pages/Dashboard';
import Research from './pages/Research';
import ReportDetail from './pages/ReportDetail';
import WatchListPage from './pages/WatchList';
import History from './pages/History';
import SearchPage from './pages/Search';
import IntelSources from './pages/IntelSources';
import IntelCollect from './pages/IntelCollect';
import IntelArchives from './pages/IntelArchives';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 2,
      staleTime: 30_000,
    },
  },
});

const App: React.FC = () => (
  <QueryClientProvider client={queryClient}>
    <BrowserRouter>
      <Routes>
        <Route element={<AppLayout />}>
          <Route path="/" element={<Dashboard />} />
          <Route path="/research" element={<Research />} />
          <Route path="/report/:stockCode/:date" element={<ReportDetail />} />
          <Route path="/watch" element={<WatchListPage />} />
          <Route path="/history" element={<History />} />
          <Route path="/search" element={<SearchPage />} />
          <Route path="/intel/sources" element={<IntelSources />} />
          <Route path="/intel/collect" element={<IntelCollect />} />
          <Route path="/intel/archives" element={<IntelArchives />} />
        </Route>
      </Routes>
    </BrowserRouter>
  </QueryClientProvider>
);

export default App;
