'use strict';

angular.module('biggraph')
  .controller('ProjectViewCtrl', function ($scope, $routeParams, $resource, $location, util) {
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
      } else{
        this.project = undefined;
      }
    };
    function clearState() {
      $scope.leftToRightPath = undefined;
      $scope.left = new Side();
      $scope.right = new Side();
      $scope.left.state.projectName = $routeParams.project;
    }
    clearState();
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

    util.deepWatch($scope, 'left.project', function(project) {
      if (project === undefined) { return; }
      // Put vertex set and edge bundle in the state.
      // This is for compatibility with the metaGraph.js-related code in graph-view.js
      // and could be removed later.
      $scope.left.state.vertexSet = { id: project.vertexSet };
      if (project.edgeBundle !== '') {
        $scope.left.state.edgeBundle = { id: project.edgeBundle };
      } else {
        $scope.left.state.edgeBundle = undefined;
      }
    });

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
      function(state) {
        $location.search({ q: JSON.stringify(state) });
      });
  });
