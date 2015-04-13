'use strict';

angular
  .module('biggraph', [
    'ngResource',
    'ngRoute',
    'ui.bootstrap',
    'ui.layout',
    'cfp.hotkeys',
    'jmdobry.angular-cache',
  ])
  .config(function ($routeProvider) {
    $routeProvider
      .when('/', {
        templateUrl: 'views/splash.html',
        controller: 'SplashCtrl',
      })
      .when('/project/:project', {
        templateUrl: 'views/project.html',
        controller: 'ProjectViewCtrl',
        reloadOnSearch: false,
      })
      .when('/metaGraph', {
        templateUrl: 'views/metaGraph.html',
        controller: 'MetaGraphViewCtrl',
        reloadOnSearch: false,
      })
      .when('/cluster', {
        templateUrl: 'views/clusterManager.html',
        controller: 'ClusterManagerCtrl',
      })
      .when('/demoMode', {
        templateUrl: 'views/demoMode.html',
        controller: 'DemoModeCtrl',
      })
      .when('/login', {
        templateUrl: 'views/login.html',
        controller: 'LoginCtrl',
      })
      .when('/users', {
        templateUrl: 'views/users.html',
        controller: 'UsersCtrl',
      })
      .otherwise({
        redirectTo: '/',
      });
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
  .factory('util', function utilFactory(
        $location, $window, $resource, $rootScope, $angularCacheFactory, $modal) {
    var siSymbols = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];
    // A persistent cache. Requests made through util.get() will not be repeated
    // even if the browser is restarted.
    var localCache = $angularCacheFactory('localCache', {
      maxAge: 24 * 3600,
      // TODO: Temporarily disabled, because this makes testing confusing during development.
      // storageMode: 'localStorage',
    });
    function ajax(url, params, cache) {
      if (params === undefined) { params = { fake: 1 }; }
      var res = $resource(url, {}, { get: { method: 'GET', cache: cache } });
      var req = res.get({ q: params }, function() {}, function(failure) {
        req.$status = failure.status;
        if (failure.status === 401) {  // Unauthorized.
          req.$error = 'Redirecting to login page.';
          if ($location.protocol() === 'https') {
            $location.url('/login');
          } else {
            $window.location.href = 'https://' + $location.host() + '/login';
          }
        } else {
          req.$error = util.responseToErrorMessage(failure);
        }
      });
      // Helpful for debugging/error reporting.
      req.$url = url;
      req.$params = params;
      return req;
    }
    var util = {
      // This function is for code clarity, so we don't have a mysterious "true" argument.
      deepWatch: function(scope, expr, fun) {
        return scope.$watch(expr, fun, true);
      },
      // Json GET with caching and parameter wrapping.
      get: function(url, params) { return ajax(url, params, localCache); },
      // Json GET with parameter wrapping and no caching.
      nocache: function(url, params) { return ajax(url, params, false); },
      // Json POST with simple error handling.
      post: function(url, params, onSuccess) {
        var resource = $resource(url).save({}, params, onSuccess, function(failure) {
          util.ajaxError(failure);
        });
        // Helpful for debugging/error reporting.
        resource.$url = url;
        resource.$params = params;
        // Returns a promise of the success state, for flexibility.
        return resource.$promise
          .then(function() { return true; }, function() { return false; });
      },
      // Easier to read numbers. 1234 -> 1k
      human: function(x) {
        if (x === undefined) { return '?'; }
        if (typeof x !== 'number') { return x; }
        if (isNaN(x)) { return x; }
        for (var i = 0; true; ++i) {
          if (x < 1000 || i === siSymbols.length - 1) {
            return x + siSymbols[i];
          }
          x = Math.round(x / 1000);
        }
      },
      // Replaces underscores with spaces.
      spaced: function(s) {
        return s.replace(/_/g, ' ');
      },
      ajaxError: function(resp) {
        util.error(
          util.responseToErrorMessage(resp),
          { request: resp.config.url, data: resp.config.data });
      },
      error: function(message, details) {
        $rootScope.$broadcast('topAlert', { message: message, details: details });
      },
      responseToErrorMessage: function(resp) {
        if (resp.data) {
          if (resp.data.error) {
            return resp.data.error;
          }
          return resp.data;
        }
        return resp.config.url + ' ' + (resp.statusText || 'failed');
      },
      scopeTitle: function(scope, titleExpr) {
        scope.$watch(titleExpr, function(title) {
          angular.element('title').html(title);
        });
        scope.$on('$destroy', function() {
          angular.element('title').html('LynxKite');
        });
      },
      reportRequestError: function(request, details) {
        if (request) {
          util.reportError({
            message: request.$error,
            details: {
              url: request.$url,
              params: request.$params,
              details: details,
            },
          });
        } else {
          util.reportError({
            message: 'undefined request',
            details: details,
          });
        }
      },
      reportError: function(alert) {
        $modal.open({
          templateUrl: 'report-error.html',
          controller: 'ReportErrorCtrl',
          resolve: { alert: function() { return alert; } },
        });
      },
      projectPath: function(projectName) {
        // A segmentation path looks like this:
        //   <parent>/checkpointed/segmentations/<segmentation>/project
        if (!projectName) { return []; }
        var parts = projectName.split('/');
        var path = [util.spaced(parts.shift())];  // Root project name.
        while (parts.length > 0) {
          if (parts[0] !== 'checkpointed') { console.error('Cannot parse', projectName); }
          parts.shift();  // "checkpointed"
          if (parts[0] !== 'segmentations') { console.error('Cannot parse', projectName); }
          parts.shift();  // "segmentations"
          path.push(util.spaced(parts.shift()));  // segmentation name
          if (parts[0] !== 'project') { console.error('Cannot parse', projectName); }
          parts.shift();  // "project"
        }
        return path;
      }
    };
    util.globals = util.get('/ajax/getGlobalSettings');
    return util;
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
  });
