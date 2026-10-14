import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const backendTarget = process.env.VITE_BACKEND_TARGET || 'http://backend:8080';
const wsTarget = process.env.VITE_WS_TARGET || 'ws://backend:8080';

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 3000,
    proxy: {
      '/ws': {
        target: wsTarget,
        ws: true,
        changeOrigin: true,
      },
      '/api': {
        target: backendTarget,
        changeOrigin: true,
      },
      '/metrics': {
        target: backendTarget,
        changeOrigin: true,
      },
    },
  },
});
