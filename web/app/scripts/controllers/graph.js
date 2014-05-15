'use strict';

angular.module('biggraph')
  .controller('GraphViewCtrl', function (
      $scope, $routeParams, $resource, $modal, $controller, $location) {
    var id = $routeParams.graph;
    var request = {id: id};
    var requestJson = JSON.stringify(request);
    var Graph = $resource('/ajax/graph?q=:request');
    var DerivedGraph = $resource('/ajax/derive?q=:request');
    var Stats = $resource('/ajax/stats?q=:request');

    $scope.graph = Graph.get({request: requestJson});
    $scope.stats = Stats.get({request: requestJson});

    $scope.modalResult = 'nothing';
    $scope.openModal = function(operation) {
      alert(JSON.stringify(operation))
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
	var deriveRequest = {
	  sourceIds: [id],
	  operation: {
	    operationId: operation.operationId,
	    parameters: result
	  }
	};
	var deriveRequestJson = JSON.stringify(deriveRequest);
	alert(deriveRequestJson)
	$scope.modalResult = result
	DerivedGraph.get({request: deriveRequestJson}, function(derivedGraph) {
	  $location.url('/graph/' + derivedGraph.id)
	})
      });
    }
  });
