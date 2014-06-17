'use strict';

angular
  .module('biggraph', [
    'ngResource',
    'ngRoute',
    'ui.bootstrap'
  ])
  .config(function ($routeProvider) {
    $routeProvider
      .when('/metaGraph/:vertexSet', {
        templateUrl: 'views/metaGraph.html',
        controller: 'MetaGraphViewCtrl'
      })
      .when('/cluster/:password', {
        templateUrl: 'views/clusterManager.html',
        controller: 'ClusterManagerCtrl'
      })
      .otherwise({
        redirectTo: '/metaGraph/x'
      });
  });
