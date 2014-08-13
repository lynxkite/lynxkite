'use strict';

angular.module('biggraph')
  .controller('ProjectViewCtrl', function ($scope, $routeParams, $resource, $location, util) {
    $scope.util = util;
    function defaultSideState() {
      return {
        projectName: undefined,
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
    Side.prototype.set = function(setting, value) {
      if (this.state[setting] === value) {
        // Clicking the same attribute setting again turns it off.
        delete this.state[setting];
      } else {
        this.state[setting] = value;
      }
    };
    Side.prototype.close = function() {
      this.state.projectName = undefined;
      for (var i = 0; i < $scope.sides.length; ++i) {
        console.log($scope.sides[i].state.projectName);
        if ($scope.sides[i].state.projectName !== undefined) {
          return;
        }
      }
      $location.url('/');
    };

    Side.prototype.nonEmptyFilters = function() {
      var res = [];
      for (var attr in this.state.filters) {
        if (this.state.filters[attr] !== '') {
          res.push({ attributeId: attr, valueSpec: this.state.filters[attr] });
        }
      }
      return res;
    };
    Side.prototype.hasFilters = function() {
      return this.nonEmptyFilters().length !== 0;
    };
    Side.prototype.applyFilters = function() {
      var that = this;
      $resource('/ajax/filterProject').save(
        {
          project: this.state.projectName,
          filters: this.nonEmptyFilters()
        },
        function(result) {
          if (result.success) {
            that.state.filters = {};
            that.reload();
          } else {
            console.error(result.failureReason);
          }
        },
        function(response) {
          console.error(response);
        });
    };

    $scope.left = new Side();
    $scope.right = new Side();
    $scope.sides = [$scope.left, $scope.right];
    $scope.$watch('left.state.projectName', function() { $scope.left.reload(); });
    $scope.$watch('right.state.projectName', function() { $scope.right.reload(); });

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
        if (after === before) {
          return;  // Do not modify URL on initialization.
        }
        if ($location.path().indexOf('/project/') === -1) {
          return;  // Navigating away. Leave the URL alone.
        }
        $location.search({ q: JSON.stringify(after) });
      });
  });
