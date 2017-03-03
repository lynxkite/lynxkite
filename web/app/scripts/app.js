// Creates the "biggraph" Angular module, sets the routing table, provides utility filters.
'use strict';

angular
  .module('biggraph', [
    'ngRoute',
    'ui.ace',
    'ui.bootstrap',
    'ui.layout',
    'cfp.hotkeys',
  ])

  .config(function ($routeProvider) {
    function docTemplate(doc) {
      return { template:
        '<div class="documentation">' +
        '<div documentation="' + doc + '" class="help container"></div>' +
        '</div>', reloadOnSearch: false };
    }
    function docWebTemplate(doc) {
      return { template:
        '<find-in-page-box></find-in-page-box>' +
        docTemplate(doc).template
      };
    }
    // One-page routing for PDF generation.
    if (location.pathname.indexOf('/pdf-') === 0) {
      var page = location.pathname.replace('/pdf-', '');
      $routeProvider.otherwise(docTemplate(page));
      return;
    }

    $routeProvider
      .when('/', {
        templateUrl: 'views/splash.html',
        controller: 'SplashCtrl',
      })
      .when('/workspace/:workspaceName*', {
        templateUrl: 'scripts/workspace/workspace-main.html',
        controller: 'WorkspaceMainCtrl',
      })
      .when('/demoMode', {
        templateUrl: 'views/demoMode.html',
        controller: 'DemoModeCtrl',
      })
      .when('/login', {
        templateUrl: 'views/login.html',
        controller: 'LoginCtrl',
      })
      .when('/changepassword', {
        templateUrl: 'views/changepassword.html',
        controller: 'ChangePasswordCtrl',
      })
      .when('/users', {
        templateUrl: 'views/users.html',
        controller: 'UsersCtrl',
      })
      .when('/cleaner', {
        templateUrl: 'views/cleaner.html',
        controller: 'CleanerCtrl',
      })
      .when('/backup', {
        templateUrl: 'views/backup.html',
        controller: 'BackupCtrl',
      })
      .when('/logs', {
        templateUrl: 'views/logs.html',
        controller: 'LogsCtrl',
      })
      .otherwise({
        redirectTo: '/',
      });

    // Register routing for documentation pages.
    var docs = ['academy', 'admin-manual', 'help'];
    for (var i = 0; i < docs.length; ++i) {
      $routeProvider.when('/' + docs[i], docWebTemplate(docs[i]));
    }
  })

  .config(function($httpProvider) {
    // Identify requests from JavaScript by a header.
    $httpProvider.defaults.headers.common['X-Requested-With'] = 'XMLHttpRequest';
  })

  .factory('$exceptionHandler', function($log, $injector) {
    return function(error) {
      // Log as usual.
      $log.error.apply($log, arguments);
      // Send to server.
      // (The injector is used to avoid the circular dependency detection.)
      $injector.get('util').post('/ajax/jsError', {
        url: window.location.href,
        stack: error.stack,
      });
    };
  })

  // selectFields adds a new $selection attribute to the objects, that is a newline-delimited
  // concatenation of the selected fields. This can be used to filter by searching in multiple
  // fields. For example to search in p.name and p.notes at the same time:
  //   p in projects | selectFields:'name':'notes' | filter:{ $selection: searchString }
  .filter('selectFields', function() {
    return function(input) {
      if (input === undefined) { return input; }
      for (var i = 0; i < input.length; ++i) {
        input[i].$selection = '';
        for (var j = 1; j < arguments.length; ++j) {
          input[i].$selection += input[i][arguments[j]];
          input[i].$selection += '\n';
        }
      }
      return input;
    };
  })

  .filter('trustAsHtml', function($sce) {
    return $sce.trustAsHtml;
  })

  .filter('decimal', function() {
    return function(x) {
      if (x === undefined) { return x; }
      var str = x.toString();
      var l = str.length;
      var result = '';
      for (var i = 0; i < l - 3; i += 3) {
        result = ',' + str.substr(l - i - 3, 3) + result;
      }
      result = str.substr(0, l - i) + result;
      return result;
    };
  })

  // Makes the string suitable for use as an HTML id attribute.
  .filter('id', function() {
    return function(x) {
      if (x === undefined) { return x; }
      return x.toLowerCase().replace(/ /g, '-');
    };
  })

  .filter('urlencode', function() {
    return function(x) {
      if (x === undefined) { return x; }
      return encodeURIComponent(x);
    };
  });
