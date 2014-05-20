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
      .otherwise({
        redirectTo: '/graph/x'
      });
  });
