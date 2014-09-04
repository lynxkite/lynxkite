'use strict';

angular
  .module('biggraph', [
    'ngResource',
    'ngRoute',
    'ui.bootstrap'
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
      .when('/cluster/:password', {
        templateUrl: 'views/clusterManager.html',
        controller: 'ClusterManagerCtrl',
      })
      .otherwise({
        redirectTo: '/',
      });
  })
  .factory('util', function utilFactory($resource) {
    var siSymbols = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];
    function ajax(url, params, cache) {
      if (params === undefined) { params = { fake: 1 }; }
      var res = $resource(url, {}, { get: { method: 'GET', cache: cache } });
      var req = res.get({ q: params }, function() {}, function(failure) {
        if (failure.status === 401) {  // Unauthorized.
          req.error = 'Redirecting to login page.';
          window.location.href = '/authenticate/google';
        } else {
          req.error = 'Request failed: ' + (failure.data.error || failure.data);
        }
      });
      return req;
    }
    return {
      // This function is for code clarity, so we don't have a mysterious "true" argument.
      deepWatch: function(scope, expr, fun) {
        scope.$watch(expr, fun, true);
      },
      // Json GET with caching and parameter wrapping.
      get: function(url, params) { return ajax(url, params, true); },
      // Json GET with parameter wrapping and no caching.
      nocache: function(url, params) { return ajax(url, params, false); },
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
    };
  });
