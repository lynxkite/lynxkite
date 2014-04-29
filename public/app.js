angular.module('graphs', ['ngRoute', 'ngResource'])
  .config(function($routeProvider) {
    $routeProvider
      .when('/graph/:graph', {
        controller:'GraphViewCtrl',
        templateUrl:'/graph.html',
      })
      .otherwise({
        redirectTo:'/graph/start',
      });
  });
