'use strict';

angular.module('biggraph')
  .controller('MetaGraphViewCtrl', function ($scope, $routeParams, $resource, $modal, $location) {
    var id = $routeParams.vertexSet;

    var VertexSet = $resource('/ajax/vertexSet');
    function loadGraph() {
      VertexSet.get(
        {q: {id: id}},
        function(vertexSet) {
          $scope.vertexSet = vertexSet;
          $scope.allOps = $scope.startingOps.concat(vertexSet.ops);
        });
    }

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

    var ApplyOperation = $resource('/ajax/applyOp');
    function applyOperation(operation, modalResult) {
      var request = {
        id: operation.id,
        parameters: modalResult
      };
      ApplyOperation.get({q: request}, function(fake) {
        loadGraph()
      });
    }

    function applyOperationFlow(operation) {
      openOperationModal(operation).then(function(modalResult) {
        applyOperation(operation, modalResult);
      });
    }

    var StartingOps = $resource('/ajax/startingOps');
    $scope.startingOps = StartingOps.query({q: {fake: 0}});
    $scope.allOps = $scope.startingOps;
    $scope.apply = applyOperationFlow;

    if (id !== 'x') {
      $scope.id = id;
      loadGraph(id)
    }
  });
