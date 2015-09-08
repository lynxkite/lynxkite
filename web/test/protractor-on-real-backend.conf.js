exports.config = {
  directConnect: true,
  baseUrl: 'http://localhost:9000/',
  specs: ['real-backend/*-test.js'],
  // The real backend is real slow.
  getPageTimeout: 120000,
  allScriptsTimeout: 120000,
};
