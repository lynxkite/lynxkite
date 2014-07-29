'use strict';

angular.module('biggraph')
  .controller('ProjectViewCtrl', function ($scope, $routeParams, $resource, $location) {
    function get(url, params) {
      var req = $resource(url).get(params, function() {}, function(failure) {
        req.error = 'Request failed: ' + failure.data;
      });
      return req;
    }

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
    $scope.ops = get('/ajax/ops');
    $scope.left.data = get('/ajax/project', { project: $routeParams.project });
    $scope.left.state = defaultSideState();
    $scope.right.state = defaultSideState();

    angular.deepWatch(
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

    angular.deepWatch(
      $scope,
      getState,
      function(state) {
        $location.search({ q: JSON.stringify(state) });
      });
  });
