'use strict';

angular.module('biggraph')
  .controller('GraphViewCtrl', function ($scope, $routeParams, $resource, $modal, $controller) {
    var id = $routeParams.graph;
    var request = {id: id};
    var requestJson = JSON.stringify(request);
    var Graph = $resource('/ajax/graph?q=:request');
    $scope.graph = Graph.get({request: requestJson});
    var Stats = $resource('/ajax/stats?q=:request');
    $scope.stats = Stats.get({request: requestJson});

    $scope.modalResult = 'nothing'
    $scope.openModal = function(operation) {
      var modalInstance = $modal.open({
	templateUrl: 'views/operationParameters.html',
	controller: 'OperationParametersCtrl',
	resolve: {
	  operation: function() {
	    return operation
	  }
	}
      });

      modalInstance.result.then(function (result) {
	$scope.modalResult = result
      });
    }
  });
