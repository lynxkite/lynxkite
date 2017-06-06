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

        scope.plainParamValues = {};
        scope.parametricParamValues = {};
        scope.parametricFlags = {};

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
          scope.error = error.data;
        };
        scope.reportError = function(error) {
          util.reportError({ message: error });
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
          if (scope.box === undefined ||
              !angular.equals(box.instance, scope.box.instance) /* parameters were changed */ ||
              outputStatesDiffer(box, scope.box) /* visualization editor needs this */) {
            onBlurNow(); // Switching to a different box is also "blur".
            scope.box = box;
          }
          if (!angular.equals(boxMeta, scope.boxMeta)) {
            scope.boxMeta = boxMeta;
          }
          scope.error = undefined;
          if (!box) {
            return;
          }

          // Make a copy of the parameter values.
          var paramValues = Object.assign({}, box.instance.parameters);
          var parametricParamValues = Object.assign({}, box.instance.parametricParameters);
          var parametricFlags = {};

          // Copy defaults for unset parameters.
          for (var i = 0; i < boxMeta.parameters.length; ++i) {
            var p = boxMeta.parameters[i];
            var id = p.id;
            if (paramValues[id] !== undefined ||
                parametricParamValues[id] !== undefined) {
              // Parameter p is not unset
            } else if (p.options.length === 0) {
              paramValues[id] = p.defaultValue;
            } else if (p.multipleChoice) {
              paramValues[id] = '';
            } else {
              paramValues[id] = p.options[0].id;
            }
          }

          // Re-establish parametric flags.
          for (var k = 0; k < boxMeta.parameters.length; ++k) {
            var id2 = boxMeta.parameters[k].id;
            if (parametricParamValues[id2] !== undefined) {
              parametricFlags[id2] = true;
            } else {
              parametricFlags[id2] = false;
            }
          }

          if (!angular.equals(paramValues, scope.paramValues) ||
              !angular.equals(parametricParamValues, scope.parametricParamValues) ||
              !angular.equals(parametricFlags, scope.parametricFlags)) {
            scope.plainParamValues = paramValues;
            scope.parametricParamValues = parametricParamValues;
            scope.parametricFlags = parametricFlags;
          }
        };

        function onBlurNow() {
          if (scope.box) {
            scope.workspace.updateBox(
                scope.box.instance.id,
                scope.plainParamValues,
                scope.parametricParamValues);
          }
        }

        // Updates the workspace with the parameter changes after allowing for a digest loop to
        // bubble them up from the directives.
        scope.onBlur = function() {
          $timeout(onBlurNow);
        };
      },
    };
  });
