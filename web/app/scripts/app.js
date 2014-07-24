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
  });

// This function is for code clarity, so we don't have a mysterious "true" argument.
function deepWatch(scope, expr, fun) {
  scope.$watch(expr, fun, true);
}
