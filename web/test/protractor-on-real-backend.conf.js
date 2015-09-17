exports.config = {
  directConnect: true,
  baseUrl: 'http://localhost:9000/',
  specs: ['real-backend/declarative-test.js'],
  // The real backend can be real slow.
  getPageTimeout: 120000,
  allScriptsTimeout: 120000,
  jasmineNodeOpts: {
    defaultTimeoutInterval: 120000,
  },
};
