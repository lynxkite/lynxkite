'use strict';

angular
  .module('biggraph', [
    'ngResource',
    'ngRoute'
  ])
  .config(function ($routeProvider) {
    $routeProvider
      .when('/graph/:graph', {
        templateUrl: 'views/graph.html',
        controller: 'GraphViewCtrl'
      })
      .otherwise({
        redirectTo: '/graph/start'
      });
  });
