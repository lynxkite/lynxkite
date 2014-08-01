'use strict';

angular.module('biggraph')
  .controller('ProjectViewCtrl', function ($scope, $routeParams, $resource, $location, util) {
    function defaultSideState() {
      return {
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

    $scope.left = {};
    $scope.right = {};
    $scope.left.reload = function() {
      $scope.left.project = util.nocache('/ajax/project', { name: $routeParams.project });
    };
    $scope.left.reload();
    $scope.left.state = defaultSideState();
    $scope.right.state = defaultSideState();

    util.deepWatch($scope, 'left.project', function(project) {
      // Put vertex set and edge bundle in the state.
      // This is for compatibility with the metaGraph.js-related code in graph-view.js
      // and could be removed later.
      $scope.left.state.vertexSet = { id: project.vertexSet };
      $scope.left.state.edgeBundle = { id: project.edgeBundle };
    });

    util.deepWatch(
      $scope,
      function() { return $location.search(); },
      function(search) {
        if (!search.q) {
          $scope.leftToRightPath = undefined;
          $scope.left.state = defaultSideState();
          $scope.right.state = defaultSideState();
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
