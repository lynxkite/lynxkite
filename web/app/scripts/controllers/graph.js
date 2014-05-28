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

    function openSaveToCSVModal() {
      var modalInstance = $modal.open({
        templateUrl: 'views/fileDialog.html',
        controller: 'FileDialogCtrl',
        resolve: {
          question: function() {
            return 'Enter directory name for saving current graph';
          }
        }
      });
      return modalInstance.result;
    }

    var SaveGraphAsCSV = $resource('/ajax/saveAsCSV?q=:request');
    function sendSaveToCSVRequest(id, path) {
      var saveRequest = {
        id: id,
        targetDirPath: path
      };
      var saveRequestJson = JSON.stringify(saveRequest);
      SaveGraphAsCSV.get({request: saveRequestJson}, function(response) {
        // TODO: report in the status bar instead once we have one.
        if (response.success) {
          window.alert('Graph saved successfully');
        } else {
          window.alert('Graph saving failed: ' + response.failureReason);
        }
      });
    }

    function saveCSVFlow(id) {
      openSaveToCSVModal().then(function(path) {
        sendSaveToCSVRequest(id, path);
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
 
      $scope.saveCSV = function() {
        saveCSVFlow(id);
      };

      $scope.openDerivationModal = function(operation) {
        deriveGraphFlow(operation, [id]);
      };
    }
  });
