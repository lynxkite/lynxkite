export default {
  root: 'app',
  build: {
    chunkSizeWarningLimit: 1_000_000,
    outDir: '../dist',
  },
  server: {
    hmr: false,
    proxy: {
      '/ajax': 'http://localhost:2200',
    },
  },
};
