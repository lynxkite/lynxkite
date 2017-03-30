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
            'workspace.selectedBoxIds',
            function() {
              if (!scope.workspace) {
                return;
              }
              // Make a copy of the parameter values.
              scope.paramValues = Object.assign(
                  {}, new Map(scope.workspace.selectedBoxes().map(function(box){
                    return [box.instance.id, box.instance.parameters];
                  })));
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
          if (!scope.workspace || !scope.workspace.selectedBoxIds) {
            return;
          }
          // The below magic makes sure that the response
          // to the result of the latest getOperationMetaRequest
          // will be passed to scope.newOpSelected().
          var currentRequest;
          var success = function(boxId, boxMeta) {
            if (scope.lastRequest === currentRequest) {
              scope.newOpSelected(boxMeta);
            }
          };
          var error = function(){
            if (scope.lastRequest === currentRequest) {
              scope.newOpSelected(undefined);
            }
          };
          for (var i = 0; i < scope.workspace.selectedBoxIds.length; i ++){
            scope.lastRequest = currentRequest = util
              .nocache(
                '/ajax/getOperationMeta',
                {
                  workspace: scope.workspace.name,
                  box: scope.workspace.selectedBoxIds[i],
                })
                .then(success.bind(null,scope.workspace.selectedBoxIds[i]),error);
          }};

        // Invoked when the user selects a new operation and its
        // metadata is successfully downloaded.
        scope.newOpSelected = function(boxId, boxMeta) {
            scope.boxMeta = boxMeta;
            if (!scope.boxMeta) {
              return;
            }

            // Populate parameter values.
            for (var i = 0; i < boxMeta.parameters.length; ++i) {
              var p = boxMeta.parameters[i];
              if (scope.paramValues[boxId][p.id] !== undefined) {
                // Parameter is set externally.
              } else if (p.options.length === 0) {
                scope.paramValues[boxId][p.id] = p.defaultValue;
              } else if (p.multipleChoice) {
                scope.paramValues[boxId][p.id] = '';
              } else {
                scope.paramValues[boxId][p.id] = p.options[0].id;
              }
            }
        };

        scope.apply = function() {
          scope.workspace.updateSelectedBoxes(scope.paramValues);
        };
      },
    };
});
