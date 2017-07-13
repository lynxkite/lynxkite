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
          var box = scope.workspace.getBox(boxId);
          // Checking currentRequest makes sure that the response
          // to the result of the latest getOperationMetaRequest
          // will be passed to scope.newOpSelected().
          var currentRequest;
          scope.lastRequest = currentRequest = util
            .nocache(
              '/ajax/getOperationMeta', {
                workspace: scope.workspace.ref(),
                box: boxId,
              })
            .then(
              function success(boxMeta) {
                if (scope.lastRequest === currentRequest) {
                  scope.newOpSelected(box, boxMeta);
                }
              },
              function error(error) {
                if (scope.lastRequest === currentRequest) {
                  scope.boxError(error);
                }
              });
        };

        // Invoked when an error happens after the user selected a new operation.
        scope.boxError = function(error) {
          onBlurNow();
          scope.box = undefined;
          scope.boxMeta = undefined;
          scope.error = util.responseToErrorMessage(error);
        };

        function outputStatesDiffer(box1, box2) {
          if (box1.outputs.length !== box2.outputs.length) {
            return true;
          }
          for (var i = 0; i < box1.outputs.length; ++i) {
            if (box1.outputs[i].stateId !== box2.outputs[i].stateId) {
              return true;
            }
          }
          return false;
        }

        // Invoked when the user selects a new operation and its metadata is
        // successfully downloaded. Both box and boxMeta has to be defined.
        scope.newOpSelected = function(box, boxMeta) {
          // We avoid replacing the objects if the data has not changed.
          // This is to avoid recreating the DOM for the parameters. (Which would lose the focus.)
          // We replace objects in the following cases:
          // - box data did not exist before (box editor initialization)
          // - box data was changed (box parameter change)
          // - output state of the box was changed (In this case
          //   the visualization state editors need to be updated,
          //   because they are using the project state from the output
          //   of the operation.)
          if (scope.box === undefined ||
              !angular.equals(box.instance, scope.box.instance) ||
              outputStatesDiffer(box, scope.box)) {
            onBlurNow(); // Switching to a different box is also "blur".
            scope.box = box;
          }
          if (!angular.equals(boxMeta, scope.boxMeta)) {
            scope.boxMeta = boxMeta;
          }
          scope.error = undefined;
          scope.parameters = angular.copy(scope.box.instance.parameters);
          scope.parametricParameters = angular.copy(scope.box.instance.parametricParameters);
        };

        function onBlurNow() {
          if (scope.box) {
            scope.workspace.updateBox(
                scope.boxId,
                scope.parameters,
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
          for (var k = 0; k < scope.boxMeta.parameters.length; ++k) {
            var p = scope.boxMeta.parameters[k];
            if (p.kind === 'code' && p.payload.enableTableBrowser === true) {
              return true;
            }
          }
          return false;
        };
      },
    };
  });
