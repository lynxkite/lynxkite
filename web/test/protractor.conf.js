// eslint-disable camelcase
exports.config = {
  framework: 'jasmine2',
  directConnect: true,
  specs: ['declarative.js'],
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
      args: [
        '--disable-gpu', // For #3039.
        '--no-sandbox', // Sandboxing fails on GCE in Docker. Too safe already?
      ],
      // Set download path and avoid prompting for download even though
      // this is already the default on Chrome but for completeness
      prefs: {
        download: {
          prompt_for_download: false,
          default_directory: '/tmp/protractorDownloads.' + process.pid,
        },
      },
    },
  },
};
