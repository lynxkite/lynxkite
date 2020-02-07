'use strict';

// Viewer and editor of a box instance.

angular.module('biggraph')
  .directive('boxEditor', function($timeout, util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/box-editor.html',
      scope: {
        workspace: '=',
        boxId: '=',
      },
      link: function(scope) {
        // The metadata (param definition list) of the current box
        // depends on the whole workspace. (Attributes added by
        // previous operations, state of apply_to_ parameters of
        // current box.) If this deepwatch is a performance problem,
        // then we can put a timestamp in workspace and watch that
        // instead of workspace.backendState.
        util.deepWatch(
          scope,
          '[workspace.backendState, boxId]',
          function() {
            if (!scope.boxId) {
              return;
            }
            scope.loadBoxMeta(scope.boxId);
          });

        scope.loadBoxMeta = function(boxId) {
          if (!scope.workspace) {
            return;
          }
          if (!boxId) {
            scope.box = undefined;
            scope.boxMeta = undefined;
            return;
          }
          const box = scope.workspace.getBox(boxId);
          // Checking currentRequest makes sure that the response
          // to the result of the latest getOperationMetaRequest
          // will be passed to scope.updateParams().
          let currentRequest;
          scope.lastRequest = currentRequest = util
            .nocache(
              '/ajax/getOperationMeta', {
                workspace: scope.workspace.ref(),
                box: boxId,
              })
            .then(
              function success(boxMeta) {
                if (scope.lastRequest === currentRequest) {
                  scope.updateParams(box, boxMeta);
                }
              },
              function error(error) {
                if (scope.lastRequest === currentRequest) {
                  scope.boxError(error);
                }
              });
        };

        // Invoked when an error happens while loading the metadata.
        scope.boxError = function(error) {
          onBlurNow();
          scope.box = undefined;
          scope.boxMeta = undefined;
          scope.error = util.responseToErrorMessage(error);
        };

        function threeWayMerge(before, after, local) {
          const merged = {};
          for (let k in after) {
            if (before) {
              // Keep local (possibly modified) state if there's no change from the backend.
              merged[k] = after[k] === before[k] ? local[k] : after[k];
            } else {
              merged[k] = after[k];
            }
          }
          return merged;
        }

        scope.updateParams = function(box, boxMeta) {
          scope.error = undefined;
          const parameters = {};
          for (let k in box.instance.parameters) {
            if (scope.box) {
              // Keep local (possibly modified) state if there's no change from the backend.
              parameters[k] = (box.instance.parameters[k] === scope.box.instance.parameters[k] ?
                scope.parameters[k] : box.instance.parameters[k]);
            } else {
              parameters[k] = box.instance.parameters[k];
            }
          }
          scope.parameters = threeWayMerge(
            scope.box && scope.box.instance.parameters, box.instance.parameters, scope.parameters);
          scope.parametricParameters = threeWayMerge(
            scope.box && scope.box.instance.parametricParameters, box.instance.parametricParameters,
            scope.parametricParameters);
          scope.box = box;
          if (!angular.equals(boxMeta, scope.boxMeta)) {
            scope.boxMeta = boxMeta;
          }
        };

        function onBlurNow() {
          if (scope.box) {
            const minimized = angular.copy(scope.parameters);
            for (let i = 0; i < scope.boxMeta.parameters.length; ++i) {
              const param = scope.boxMeta.parameters[i];
              if (minimized[param.id] === param.defaultValue) {
                // Do not record parameter values that match the default.
                delete minimized[param.id];
              }
            }
            scope.workspace.updateBox(
              scope.boxId,
              minimized,
              scope.parametricParameters);
          }
        }

        // Updates the workspace with the parameter changes after allowing for a digest loop to
        // bubble them up from the directives.
        scope.onBlur = function() {
          $timeout(onBlurNow);
        };

        scope.getBox = function() {
          return scope.workspace.boxMap[scope.boxId];
        };

        // Returns true iff the boxMeta has at least one SQL code type parameter.
        scope.withTableBrowser = function() {
          if (!scope.boxMeta) {
            return false;
          }
          for (let k = 0; k < scope.boxMeta.parameters.length; ++k) {
            const p = scope.boxMeta.parameters[k];
            if (p.kind === 'code' && p.payload.enableTableBrowser === true) {
              return true;
            }
          }
          return false;
        };
      },
    };
  });
