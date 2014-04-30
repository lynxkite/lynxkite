angular.module('graphs')
  .controller('GraphViewCtrl', function($scope, $routeParams, $resource) {
    var id = $routeParams.graph;
    var Graph = $resource('/ajax/graph/:graph');
    $scope.graph = Graph.get({graph: id});
  })
;
