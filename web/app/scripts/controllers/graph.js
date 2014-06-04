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

    var DerivedGraph = $resource('/ajax/derive');
    function jumpToDerivedGraph(operation, modalResult, sourceIds) {
      var deriveRequest = {
        sourceIds: sourceIds,
        operation: {
          operationId: operation.operationId,
          parameters: modalResult
        }
      };
      DerivedGraph.get({q: deriveRequest}, function(derivedGraph) {
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

    var SaveGraphAsCSV = $resource('/ajax/saveAsCSV');
    function sendSaveToCSVRequest(id, path) {
      var saveRequest = {
        id: id,
        targetDirPath: path
      };
      SaveGraphAsCSV.get({q: saveRequest}, function(response) {
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

    var SaveGraph = $resource('/ajax/save');
    function sendSaveRequest(id) {
      var saveRequest = {
        id: id
      };
      SaveGraph.get({q: saveRequest});
    }

    var StartingOps = $resource('/ajax/startingOps');
    $scope.startingOps = StartingOps.query({q: {fake: 0}});

    $scope.openNewGraphModal = function(operation) {
      deriveGraphFlow(operation, []);
    };

    var id = $routeParams.graph;
    if (id !== 'x') {
      var Graph = $resource('/ajax/graph');
      var Stats = $resource('/ajax/stats');

      $scope.id = id;
      $scope.graph = Graph.get({q: {id: id}});
      $scope.stats = Stats.get({q: {id: id}});
 
      $scope.saveCSV = function() {
        saveCSVFlow(id);
      };

      $scope.save = function() {
        sendSaveRequest(id);
      };

      $scope.openDerivationModal = function(operation) {
        deriveGraphFlow(operation, [id]);
      };
    }
  });
