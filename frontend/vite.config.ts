import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';

export default defineConfig({
  base: '/debugconsole/',
  plugins: [vue()],
  server: {
    port: 5173,
    open: false,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8192',
        changeOrigin: true,
      },
    },
  },
  build: {
    sourcemap: true,
    outDir: 'dist',
  },
});