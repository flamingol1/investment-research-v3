/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        brand: {
          50: '#eff6ff',
          100: '#dbeafe',
          200: '#bfdbfe',
          300: '#93c5fd',
          400: '#60a5fa',
          500: '#3b82f6',
          600: '#2563eb',
          700: '#1d4ed8',
        },
        rise: '#ef4444',    // A股涨红
        fall: '#22c55e',    // A股跌绿
        surface: {
          DEFAULT: '#1a1f2e',
          dark: '#0f1419',
          light: '#242b3d',
        },
      },
      fontFamily: {
        sans: ['-apple-system', '"PingFang SC"', '"Microsoft YaHei"', 'sans-serif'],
        mono: ['"SF Mono"', '"Fira Code"', 'monospace'],
      },
    },
  },
  plugins: [],
  // 在Ant Design组件上禁用preflight，避免冲突
  corePlugins: {
    preflight: false,
  },
};
