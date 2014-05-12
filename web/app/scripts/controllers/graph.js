'use strict';

angular.module('biggraph')
  .controller('GraphViewCtrl', function ($scope, $routeParams, $resource) {
    var id = $routeParams.graph;
    var request = {id: id};
    var requestJson = JSON.stringify(request);
    var Graph = $resource('/ajax/graph?q=:request');
    Graph.get({request: requestJson}).$promise.then(function (response) {
      $scope.graph = response;
    });
    var Stats = $resource('/ajax/stats?q=:request');
    Stats.get({request: requestJson}).$promise.then(function (response) {
      $scope.stats = response;
    });
  });
