'use strict';
import '../app';
import '../util/util';
import templateUrl from './visualization-parameter.html?url';

// Editor for a visualization state stored in a parameter string.

angular.module('biggraph')
  .directive('visualizationParameter', ['util', 'side', function(util, side) {
    return {
      restrict: 'E',
      templateUrl,
      scope: {
        projectStateId: '=',
        uiState: '=',
        onUiStateChanged: '&',
      },
      link: function(scope) {
        scope.sides = [];
        scope.left = new side.Side(scope.sides, 'left');
        scope.left.enableVisualizationUI = true;
        scope.right = new side.Side(scope.sides, 'right');
        scope.right.enableVisualizationUI = true;
        scope.sides.push(scope.left);
        scope.sides.push(scope.right);

        scope.$watch('projectStateId', function(newValue, oldValue, scope) {
          scope.left.stateId = scope.projectStateId;
          scope.right.stateId = scope.projectStateId;
          scope.applyVisualizationData();
          const leftPromise = scope.left.reload();
          const rightPromise = scope.right.reload();
          if (leftPromise) {
            leftPromise.then(function() { scope.left.onProjectLoaded(); });
          }
          if (rightPromise) {
            rightPromise.then(function() { scope.right.onProjectLoaded(); });
          }
        });

        scope.applyVisualizationData = function() {
          let state = {
            left: undefined,
            right: undefined,
          };
          if (scope.uiState) {
            state = JSON.parse(scope.uiState);
          }

          if (state.left) {
            scope.left.updateFromBackendJson(state.left);
          } else {
            scope.left.state.projectPath = '';
            scope.left.state.graphMode = 'sampled';
          }
          if (state.right) {
            scope.right.updateFromBackendJson(state.right);
          }
        };

        scope.saveBoxState = function() {
          scope.uiState = JSON.stringify({
            left: scope.left.state,
            right: scope.right.state
          });
          scope.onUiStateChanged();
        };

        util.deepWatch(
          scope,
          '[left.state, right.state]',
          function(newVal, oldVal) {
            if (oldVal === newVal) {
              // This was the initial watch call.
              return;
            }
            scope.left.updateViewData();
            scope.right.updateViewData();
            scope.saveBoxState();
          });
      },
    };
  }]);
