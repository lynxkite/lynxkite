'use strict';

angular.module('biggraph')
  .controller('ProjectViewCtrl', function ($scope, $routeParams, $resource, $location, util) {
    $scope.util = util;
    function defaultSideState() {
      return {
        projectName: undefined,
        vs: undefined,
        filters: {},
        graphMode: undefined,
        bucketCount: 4,
        sampleRadius: 1,
        center: undefined,
      };
    }

    function getState() {
      return {
        leftToRightPath: $scope.leftToRightPath,
        left: $scope.left.state,
        right: $scope.right.state,
      };
    }

    function Side() {
      this.state = defaultSideState();
    }
    Side.prototype.reload = function() {
      if (this.state.projectName) {
        this.project = util.nocache('/ajax/project', { name: this.state.projectName });
      } else {
        this.project = undefined;
      }
    };

    $scope.left = new Side();
    $scope.right = new Side();
    function clearState() {
      $scope.leftToRightPath = undefined;
      $scope.left.state = defaultSideState();
      $scope.right.state = defaultSideState();
      $scope.left.state.projectName = $routeParams.project;
    }
    $scope.$watch('left.state.projectName', function() { $scope.left.reload(); });
    $scope.$watch('right.state.projectName', function() { $scope.right.reload(); });

    function setSideSetting(side, setting, value) {
      if (side.state[setting] === value) {
        // Clicking the same attribute setting again turns it off.
        delete side.state[setting];
      } else {
        side.state[setting] = value;
      }
    }
    $scope.left.set = function(setting, value) {
      setSideSetting($scope.left, setting, value);
    };
    $scope.right.set = function(setting, value) {
      setSideSetting($scope.right, setting, value);
    };

    // Side.graphState is for compatibility with the metaGraph.js-related code in graph-view.js
    // and could be removed later.
    util.deepWatch($scope, 'left.project', function() { $scope.left.updateGraphState(); });
    util.deepWatch($scope, 'left.state', function() { $scope.left.updateGraphState(); });
    util.deepWatch($scope, 'right.project', function() { $scope.right.updateGraphState(); });
    util.deepWatch($scope, 'right.state', function() { $scope.right.updateGraphState(); });
    Side.prototype.updateGraphState = function() {
      this.graphState = angular.copy(this.state, this.graphState);
      if (this.project === undefined || !this.project.$resolved) {
        this.graphState.vertexSet = undefined;
        this.graphState.edgeBundle = undefined;
        return;
      }
      this.graphState.vertexSet = { id: this.project.vertexSet };
      if (this.project.edgeBundle !== '') {
        this.graphState.edgeBundle = { id: this.project.edgeBundle };
      } else {
        this.graphState.edgeBundle = undefined;
      }
    };

    util.deepWatch(
      $scope,
      function() { return $location.search(); },
      function(search) {
        if (!search.q) {
          clearState();
        } else {
          var state = JSON.parse(search.q);
          // The parts of the template that depend on 'state' get re-rendered
          // when we replace it. So we only do this if there is an actual
          // difference.
          if (!angular.equals(state, getState())) {
            $scope.leftToRightPath = state.leftToRightPath;
            $scope.left.state = state.left;
            $scope.right.state = state.right;
          }
        }
      });

    util.deepWatch(
      $scope,
      getState,
      function(after, before) {
        if (after !== before) {
          $location.search({ q: JSON.stringify(after) });
        }
      });
  });
