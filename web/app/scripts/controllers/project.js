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
      // The state of controls. E.g. bucket count.
      this.state = defaultSideState();
      // The /ajax/project Ajax response.
      this.project = undefined;
      // graphState is for compatibility with the metaGraph.js-related code in graph-view.js
      // and could be removed later.
      this.graphState = {};
    }
    // Side.reload makes an unconditional, uncached Ajax request.
    // This is called when the project name changes, or when the project
    // itself is expected to change. (Such as after an operation.)
    Side.prototype.reload = function() {
      if (this.state.projectName) {
        this.project = util.nocache('/ajax/project', { name: this.state.projectName });
      } else {
        this.project = undefined;
      }
    };

    $scope.left = new Side();
    $scope.right = new Side();
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
      angular.copy(this.state, this.graphState);
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

    // This watcher copies the state from the URL into $scope.
    // It is an important part of initialization. Less importantly it makes
    // it possible to edit the state manually in the URL, or use the "back"
    // button to undo state changes.
    util.deepWatch(
      $scope,
      function() { return $location.search(); },
      function(search) {
        if (!search.q) {
          $scope.leftToRightPath = undefined;
          $scope.left.state = defaultSideState();
          $scope.right.state = defaultSideState();
          // In the absence of query parameters, take the left-side project
          // name from the URL. This makes for friendlier project links.
          $scope.left.state.projectName = $routeParams.project;
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
        if (after !== before) {  // Do not modify URL on initialization.
          $location.search({ q: JSON.stringify(after) });
        }
      });
  });
