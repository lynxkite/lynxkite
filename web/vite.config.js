import { defineConfig } from 'vite';

export default defineConfig({
  root: 'app',
  build: {
    chunkSizeWarningLimit: 1_000_000,
    outDir: '../dist',
    sourcemap: true,
  },
  server: {
    hmr: false,
    proxy: {
      '/ajax': 'http://localhost:2200',
      '/download': 'http://localhost:2200',
    },
  },
});
