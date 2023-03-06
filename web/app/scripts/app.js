// Creates the "biggraph" Angular module, sets the routing table, provides utility filters.
import chroma from 'chroma-js';
import './util/jq-global';
import "angular"
import "angular-cookies"
import "angular-hotkeys";
import "angular-route"
import "angular-sanitize"
import "angular-ui-bootstrap"
import "angular-ui-ace"
import splashTemplate from './splash/splash.html?url';
import workspaceTemplate from './workspace/workspace-entry-point.html?url';
import wizardTemplate from './wizard/wizard.html?url';
import logsTemplate from './logs.html?url';
import backupTemplate from './backup.html?url';
import demoModeTemplate from './demo-mode.html?url';
import cleanerTemplate from './cleaner.html?url';

angular.module('biggraph', [
  'ngRoute',
  'ui.ace',
  'ui.bootstrap',
  'cfp.hotkeys',
]);

angular.module('biggraph').config(["$routeProvider", "$locationProvider", function ($routeProvider, $locationProvider) {
  $locationProvider.hashPrefix(''); // https://docs.angularjs.org/guide/migration#commit-aa077e8
  function docTemplate(doc, title) {
    return { template: `
      <div class="documentation">
      <div documentation="${doc}" title="${title}" class="help"></div>
      </div>
      `, reloadOnSearch: false };
  }
  // One-page routing for PDF generation.
  if (location.pathname.indexOf('/pdf-') === 0) {
    const page = location.pathname.replace('/pdf-', '');
    $routeProvider.otherwise(docTemplate(page));
    return;
  }

  $routeProvider
    .when('/', {
      templateUrl: splashTemplate,
      controller: 'SplashCtrl',
    })
    .when('/dir:directoryName*', {
      templateUrl: splashTemplate,
      controller: 'SplashCtrl',
    })
    .when('/workspace/:workspaceName*', {
      templateUrl: workspaceTemplate,
      controller: 'WorkspaceEntryPointCtrl',
    })
    .when('/wizard/:name*', {
      templateUrl: wizardTemplate,
      controller: 'WizardCtrl',
    })
    .when('/demo-mode', {
      templateUrl: demoModeTemplate,
      controller: 'DemoModeCtrl',
    })
    .when('/cleaner', {
      templateUrl: cleanerTemplate,
      controller: 'CleanerCtrl',
    })
    .when('/backup', {
      templateUrl: backupTemplate,
      controller: 'BackupCtrl',
    })
    .when('/logs', {
      templateUrl: logsTemplate,
      controller: 'LogsCtrl',
    })
    .otherwise({
      redirectTo: '/',
    });

  // Register routing for documentation pages.
  const docs = {
    'admin-manual': 'LynxKite Admin Manual',
    'help': 'LynxKite User Guide' };
  for (let k in docs) {
    $routeProvider.when('/' + k, docTemplate(k, docs[k]));
  }
}]);

angular.module('biggraph').config(["$httpProvider", function($httpProvider) {
  // Identify requests from JavaScript by a header.
  $httpProvider.defaults.headers.common['X-Requested-With'] = 'XMLHttpRequest';
}]);

// selectFields adds a new $selection attribute to the objects, that is a newline-delimited
// concatenation of the selected fields. This can be used to filter by searching in multiple
// fields. For example to search in p.name and p.notes at the same time:
//   p in projects | selectFields:'name':'notes' | filter:{ $selection: searchString }
angular.module('biggraph').filter('selectFields', function() {
  return function(input) {
    if (input === undefined) { return input; }
    for (let i = 0; i < input.length; ++i) {
      input[i].$selection = '';
      for (let j = 1; j < arguments.length; ++j) {
        input[i].$selection += input[i][arguments[j]];
        input[i].$selection += '\n';
      }
    }
    return input;
  };
});

angular.module('biggraph').filter('trustAsHtml', ["$sce", function($sce) {
  return $sce.trustAsHtml;
}]);

angular.module('biggraph').filter('decimal', function() {
  return function(x) {
    if (x === undefined) { return x; }
    const str = x.toString();
    const l = str.length;
    let result = '';
    let i;
    for (i = 0; i < l - 3; i += 3) {
      result = ',' + str.substr(l - i - 3, 3) + result;
    }
    result = str.substr(0, l - i) + result;
    return result;
  };
});

// Makes the string suitable for use as an HTML id attribute.
angular.module('biggraph').filter('id', function() {
  return function(x) {
    if (x === undefined) { return x; }
    return x.toLowerCase().replace(/[ !?,./]/g, '-');
  };
});

angular.module('biggraph').filter('urlencode', function() {
  return function(x) {
    if (x === undefined) { return x; }
    return encodeURIComponent(x);
  };
});

chroma.brewer['LynxKite Classic'] = chroma.brewer['lynxkite classic'] = [
  '#3636a1', '#4b36a1', '#6136a1', '#7636a1', '#8c36a1', '#a136a1',
  '#a1368c', '#a13676', '#a13661', '#a1364b', '#a13636'];
chroma.brewer['LynxKite Colors'] = chroma.brewer['lynxkite colors'] = [
  '#39bcf3', '#ff8800', '#ae1dd6', '#004165', '#9b2016', '#69be28',
  '#b4ffff', '#450073', '#005e00', '#ff90ff'];
chroma.brewer['Rainbow'] = chroma.brewer['rainbow'] = [
  'hsl(0,50%,42%)', 'hsl(30,50%,42%)', 'hsl(60,50%,42%)', 'hsl(90,50%,42%)',
  'hsl(120,50%,42%)', 'hsl(150,50%,42%)', 'hsl(180,50%,42%)', 'hsl(210,50%,42%)',
  'hsl(240,50%,42%)', 'hsl(270,50%,42%)', 'hsl(300,50%,42%)', 'hsl(330,50%,42%)'];
