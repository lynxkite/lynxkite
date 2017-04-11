'use strict';

// Viewer and editor of a box instance.

angular.module('biggraph')
 .directive('boxEditor', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/box-editor.html',
      scope: {
        workspace: '=',
      },
      link: function(scope) {
        scope.$watch(
            'workspace.selectedBoxIds[0]',
            function() {
              if (!scope.workspaceselectedBoxIds) {
                return;
              }
              // Make a copy of the parameter values.
              scope.paramValues = Object.assign(
                  {}, scope.workspace.selectedBoxes()[0].instance.parameters);
              scope.loadBoxMeta();
            });
        // The metadata (param definition list) of the current box
        // depends on the whole workspace. (Attributes added by
        // previous operations, state of apply_to_ parameters of
        // current box.) If this deepwatch is a performance problem,
        // then we can put a timestamp in workspace and watch that,
        // or only deepwatch the current selected box (and assume
        // box selection has to change to edit other boxes).
        scope.$watch(
            'workspace.backendState',
            function() {
              if (!scope.workspace) {
                return;
              }
              scope.loadBoxMeta();
            },
            true);

        scope.paramValues = {};

        scope.loadBoxMeta = function() {
          if (!scope.workspace || !scope.workspace.selectedBoxIds[0]) {
            return;
          }
          var box = scope.workspace.selectedBoxes()[0];
          // Checking currentRequest makes sure that the response
          // to the result of the latest getOperationMetaRequest
          // will be passed to scope.newOpSelected().
          var currentRequest;
          scope.lastRequest = currentRequest = util
            .nocache(
              '/ajax/getOperationMeta',
              {
                  workspace: scope.workspace.name,
                  box: scope.workspace.selectedBoxIds[0],
              })
            .then(
              function success(boxMeta) {
                if (scope.lastRequest === currentRequest) {
                  scope.newOpSelected(box, boxMeta);
                }
              },
              function error() {
                if (scope.lastRequest === currentRequest) {
                  scope.newOpSelected(undefined);
                }
              });
        };

        // Invoked when the user selects a new operation and its
        // metadata is successfully downloaded.
        scope.newOpSelected = function(box, boxMeta) {
            scope.box = box;
            scope.boxMeta = boxMeta;
            if (!box) {
              return;
            }

            // Populate parameter values.
            for (var i = 0; i < boxMeta.parameters.length; ++i) {
              var p = boxMeta.parameters[i];
              if (scope.paramValues[p.id] !== undefined) {
                // Parameter is set externally.
              } else if (p.options.length === 0) {
                scope.paramValues[p.id] = p.defaultValue;
              } else if (p.multipleChoice) {
                scope.paramValues[p.id] = '';
              } else {
                scope.paramValues[p.id] = p.options[0].id;
              }
            }
        };

        scope.apply = function() {
          scope.workspace.updateSelectedBox(scope.paramValues);
        };
      },
    };
});
