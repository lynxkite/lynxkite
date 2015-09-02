exports.config = {
  directConnect: true,
  baseUrl: 'http://localhost:9000/',
  specs: ['real-backend/*-test.js'],
  getPageTimeout: 120000, // The real backend is real slow.
};
