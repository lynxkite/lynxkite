export default {
  root: 'app',
  server: {
    hmr: false,
    proxy: {
      '/ajax': 'http://localhost:2200',
    },
  },
};
