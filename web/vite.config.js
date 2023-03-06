export default {
  root: 'app',
  build: {
    chunkSizeWarningLimit: 1_000_000,
    outDir: '../dist',
    minify: false,
    sourcemap: true,
  },
  server: {
    hmr: false,
    proxy: {
      '/ajax': 'http://localhost:2200',
    },
  },
};
