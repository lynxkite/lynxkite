'use strict';

angular.module('biggraph')
  .controller('GraphViewCtrl', function ($scope, $routeParams, $resource) {
    var id = $routeParams.graph;
    var request = {id: id}
    var Graph = $resource('/ajax/graph?q=:request');
    $scope.graph = Graph.get({request: JSON.stringify(request)});
    var Stats = $resource('/ajax/stats?q=:request');
    $scope.stats = Graph.get({request: JSON.stringify(request)});
  });
