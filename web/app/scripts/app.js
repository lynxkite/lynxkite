'use strict';

angular
  .module('biggraph', [
    'ngResource',
    'ngRoute',
    'ui.bootstrap'
  ])
  .config(function ($routeProvider) {
    $routeProvider
      .when('/graph/:graph', {
        templateUrl: 'views/graph.html',
        controller: 'GraphViewCtrl'
      })
      .when('/cluster/:password', {
        templateUrl: 'views/clusterManager.html',
        controller: 'ClusterManagerCtrl'
      })
      .otherwise({
        redirectTo: '/graph/x'
      });
  });
