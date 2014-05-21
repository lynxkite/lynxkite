'use strict';

angular.module('biggraph')
  .controller('GraphViewCtrl', function ($scope, $routeParams, $resource, $modal, $location) {
    function openOperationModal(operation) {
      var modalInstance = $modal.open({
        templateUrl: 'views/operationParameters.html',
        controller: 'OperationParametersCtrl',
        resolve: {
          operation: function() {
            return operation;
          }
        }
      });
      return modalInstance.result;
    }

    var DerivedGraph = $resource('/ajax/derive?q=:request');
    function jumpToDerivedGraph(operation, modalResult, sourceIds) {
      var deriveRequest = {
        sourceIds: sourceIds,
        operation: {
          operationId: operation.operationId,
          parameters: modalResult
        }
      };
      var deriveRequestJson = JSON.stringify(deriveRequest);
      DerivedGraph.get({request: deriveRequestJson}, function(derivedGraph) {
        $location.url('/graph/' + derivedGraph.id);
      });
    }

    function deriveGraphFlow(operation, sourceIds) {
      openOperationModal(operation).then(function(modalResult) {
        jumpToDerivedGraph(operation, modalResult, sourceIds);
      });
    }

    var StartingOps = $resource('/ajax/startingOps?q=:request');
    var emptyRequest = {fake: 0};
    var emptyRequestJson = JSON.stringify(emptyRequest);
    $scope.startingOps = StartingOps.query({request: emptyRequestJson});

    $scope.openNewGraphModal = function(operation) {
      deriveGraphFlow(operation, []);
    };

    var id = $routeParams.graph;
    if (id !== 'x') {
      var Graph = $resource('/ajax/graph?q=:request');
      var Stats = $resource('/ajax/stats?q=:request');

      $scope.id = id;
      var request = {id: id};
      var requestJson = JSON.stringify(request);
      $scope.graph = Graph.get({request: requestJson});
      $scope.stats = Stats.get({request: requestJson});

      $scope.openDerivationModal = function(operation) {
        deriveGraphFlow(operation, [id]);
      };
    }
  });
