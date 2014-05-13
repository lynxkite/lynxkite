'use strict';

angular.module('biggraph')
  .controller('GraphViewCtrl', function ($scope, $routeParams, $resource) {
    var id = $routeParams.graph;
    var request = {id: id};
    var requestJson = JSON.stringify(request);
    var Graph = $resource('/ajax/graph?q=:request');
    $scope.graph = Graph.get({request: requestJson});
    var Stats = $resource('/ajax/stats?q=:request');
    $scope.stats = Stats.get({request: requestJson});   
  });
