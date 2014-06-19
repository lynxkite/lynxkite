'use strict';

angular.module('biggraph')
  .controller('MetaGraphViewCtrl', function ($scope, $routeParams, $resource, $modal) {
    $scope.alerts = [];
    $scope.closeAlert = function(index) { $scope.alerts.splice(index, 1); };

    var id = $routeParams.vertexSet;

    var VertexSet = $resource('/ajax/vertexSet');
    function loadCurrentVertexSet() {
      VertexSet.get(
        {q: {id: id}},
        function(vertexSet) {
          $scope.vertexSet = vertexSet;
          $scope.allOps = $scope.startingOps.concat(vertexSet.ops);
        });
    }
    var StartingVertexSets = $resource('/ajax/startingVs');
    function loadStartingVertexSets() {
      StartingVertexSets.query(
        {q: {fake: 0}},
        function(vertexSets) {
          $scope.startingVertexSets = vertexSets;
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
      ApplyOperation.get({q: request}, function(result) {
        if (!result.success) {
          $scope.alerts.push({type: 'danger', msg: result.failureReason});
        }
        update();
      }, function(response) {
        $scope.alerts.push({type: 'danger', msg: 'Request failed: ' + response.status});
        update();
      });
      function update() {
        if (id !== 'x') {
          loadCurrentVertexSet();
        }
        loadStartingVertexSets();
      }
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
    loadStartingVertexSets();

    if (id !== 'x') {
      $scope.id = id;
      loadCurrentVertexSet();
    }
  });
