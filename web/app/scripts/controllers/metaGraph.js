'use strict';

angular.module('biggraph')
  .controller('MetaGraphViewCtrl', function ($scope, $resource, $modal, $location) {
    $scope.alerts = [];
    $scope.closeAlert = function(index) { $scope.alerts.splice(index, 1); };

    var defaultState = {
      leftVS: undefined,
      leftEB: undefined,
      rightVSId: undefined,
      rightEBId: undefined,
      leftToRightPath: undefined
    };

    $scope.left = {};
    $scope.right = {};

    $scope.left.other = $scope.right;
    $scope.right.other = $scope.left;

    $scope.$watch(
      function() { return $location.search(); },
      function() {
        if (!$location.search().q) {
          $scope.state = defaultState;
        } else {
          $scope.state = JSON.parse($location.search().q);
        }
      },
      true /* compare by equality rather than identity */);
    
    $scope.$watch(
      'state',
      function() {
        // Update URL.
        var s = $location.search();
        s.q = JSON.stringify($scope.state);
        $location.search(s);
        // Update graph view.
        if ($scope.showGraph) { loadGraphView(); }
      },
      true /* compare by equality rather than identity */);

    var VertexSet = $resource('/ajax/vertexSet');
    function loadVertexSet(id) {
      return VertexSet.get({q: {id: id}});
    }

    $scope.$watch(
      'state.leftVS',
      function() {
        var leftVS = $scope.state.leftVS;
        if (leftVS !== undefined) {
          $scope.left.data = loadVertexSet(leftVS.id);
        } else {
          $scope.left.data = undefined;
        }
      });
    $scope.$watch(
      'state.rightVS',
      function() {
        var rightVS = $scope.state.rightVS;
        if (rightVS !== undefined) {
          $scope.right.data = loadVertexSet(rightVS.id);
        } else {
          $scope.right.data = undefined;
        }
      });

    $scope.$watch('showGraph', function() {
      if ($scope.showGraph) { loadGraphView(); }
    });
    function loadGraphView() {
      $scope.graphView = $resource('/ajax/bucketed').get(); // TODO: parameters.
    }

    var StartingVertexSets = $resource('/ajax/startingVs');
    function loadStartingVertexSets() {
      $scope.startingVertexSets = StartingVertexSets.query({q: {fake: 0}});
    }

    $scope.applicableOperations = function() {
      var res = [];
      if ($scope.startingOps) {
        res = res.concat($scope.startingOps);
      }
      if ($scope.left.data) {
        res = res.concat($scope.left.data.ops);
      }
      return res;
    };

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
      ApplyOperation.get(
        {q: request},
        function(result) {
          if (!result.success) {
            $scope.alerts.push({type: 'danger', msg: result.failureReason});
          }
          update();
        },
        function(response) {
          $scope.alerts.push({type: 'danger', msg: 'Request failed: ' + response.status});
          update();
        });
      function update() {
        // Force reloads of graphs by creating new objects.
        var oldLeftVS = $scope.state.leftVS;
        if (oldLeftVS !== undefined) {
          $scope.state.leftVS = { id: oldLeftVS.id };
        }
        var oldRightVS = $scope.state.rightVS;
        if (oldRightVS !== undefined) {
          $scope.state.rightVS = { id: oldRightVS.id };
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
    loadStartingVertexSets();

    $scope.apply = applyOperationFlow;
    $scope.removePath = function() {
      $scope.state.leftToRightPath = undefined;
    };

    $scope.left.setVS = function(id) {
      $scope.state.leftVS = { id: id };
    };
    $scope.right.setVS = function(id) {
      $scope.state.rightVS = { id: id };
    };

    function setNewVS(side) {
      return function(id) {
        $scope.removePath();
        side.setVS(id);
      };
    }
    $scope.left.setNewVS = setNewVS($scope.left);
    $scope.right.setNewVS = setNewVS($scope.right);

    $scope.left.addEBToPath = function(eb, pointsTowardsMySide) {
      $scope.state.leftToRightPath.unshift({eb: eb, pointsLeft: pointsTowardsMySide});
    };
    $scope.right.addEBToPath = function(eb, pointsTowardsMySide) {
      $scope.state.leftToRightPath.push({eb: eb, pointsLeft: !pointsTowardsMySide});
    };

    function followEB(side) {
      return function(eb, pointsTowardsMySide) {
        if ($scope.state.leftToRightPath !== undefined) {
          side.addEBToPath(eb, pointsTowardsMySide);
        }
        if (pointsTowardsMySide) {
          side.setVS(eb.destination.id);
        } else {
          side.setVS(eb.source.id);
        }
      };
    }
    $scope.left.followEB = followEB($scope.left);
    $scope.right.followEB = followEB($scope.right);

    function showEB(side) {
      return function(eb, pointsTowardsMySide) {
        $scope.state.leftToRightPath = [];
        side.addEBToPath(eb, pointsTowardsMySide);
        if (pointsTowardsMySide) {
          side.other.setVS(eb.source.id);
        } else {
          side.other.setVS(eb.destination.id);
        }
      };
    }
    $scope.left.showEB = showEB($scope.left);
    $scope.right.showEB = showEB($scope.right);

    $scope.cutPathLeft = function(idx) {
      $scope.state.leftToRightPath.splice(0, idx);
      var firstStep = $scope.state.leftToRightPath[0];
      if (firstStep.pointsLeft) {
        $scope.left.setVS(firstStep.eb.destination.id);
      } else {
        $scope.left.setVS(firstStep.eb.source.id);
      }
    };
    $scope.cutPathRight = function(idx) {
      $scope.state.leftToRightPath.splice(idx + 1);
      var lastStep = $scope.state.leftToRightPath[$scope.state.leftToRightPath.length - 1];
      if (lastStep.pointsLeft) {
        $scope.right.setVS(lastStep.eb.source.id);
      } else {
        $scope.right.setVS(lastStep.eb.destination.id);
      }
    };
  });
