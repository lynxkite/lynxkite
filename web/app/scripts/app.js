'use strict';

angular
  .module('biggraph', [
    'ngResource',
    'ngRoute',
    'ui.bootstrap'
  ])
  .run(function($rootScope) {
    var siSymbols = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];
    $rootScope.human = function(x) {
      for (var i = 0; true; ++i) {
        if (x < 1000 || i === siSymbols.length - 1) {
          return x + siSymbols[i];
        }
        x = Math.round(x / 1000);
      }
    };
  })
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
  // This function is for code clarity, so we don't have a mysterious "true" argument.
  .factory('deepWatch', function deepWatchFactory() {
    return function(scope, expr, fun) {
      scope.$watch(expr, fun, true);
    };
  })
  // Json GET with caching and parameter wrapping.
  .factory('get', function getFactor($resource) {
    return function(url, params) {
      if (params === undefined) { params = { fake: 1 }; }
      var res = $resource(url, {}, { get: { method: 'GET', cache: true } });
      var req = res.get({ q: params }, function() {}, function(failure) {
        req.error = 'Request failed: ' + failure.data;
      });
      return req;
    };
  });
