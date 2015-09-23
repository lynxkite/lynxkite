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
  capabilities: {
    browserName: 'chrome',
    platform: 'ANY',
    version: 'ANY',
    chromeOptions: {
      // Set download path and avoid prompting for download even though
      // this is already the default on Chrome but for completeness
        prefs: {
          download: {
            prompt_for_download: false,
            default_directory: '/tmp',
          },
        },
     },
  },
};
