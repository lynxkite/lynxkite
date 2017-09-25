// Box metadata editor.
'use strict';

angular.module('biggraph')
  .directive('boxMetaeditor', function($timeout) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/box-metaeditor.html',
      scope: {
        workspace: '=',
        boxId: '=',
        popupModel: '=',
      },
      link: function(scope, element) {
        element.on('focusout', function() { scope.onBlur(); });

        function onBlurNow() {
          var box = scope.workspace.boxMap[scope.boxId];
          var changed = false;
          if (box.instance.operationId !== scope.metadata.operationId) {
            changed = true;
            box.instance.operationId = scope.metadata.operationId;
          }
          if (box.instance.id !== scope.metadata.id) {
            changed = true;
            var newId = scope.metadata.id;
            box.instance.id = newId;
            // Update connections.
            for (var i = 0; i < box.outputs.length; ++i) {
              var src = box.outputs[i];
              var dsts = src.getAttachedPlugs();
              for (var j = 0; j < dsts.length; ++j) {
                var dst = dsts[j];
                dst.boxInstance.inputs[dst.id] = { boxId: newId, id: src.id };
              }
            }
            scope.workspace._buildArrows();
            // Update this editor.
            scope.boxId = newId;
          }
          if (changed) {
            scope.workspace.saveWorkspace();
            // Update the popup.
            scope.popupModel.id = scope.metadata.id;
            scope.popupModel.title = scope.metadata.operationId;
            scope.popupModel.content.boxId = scope.metadata.id;
          }
        }

        // Updates the workspace with the parameter changes after allowing for a digest loop to
        // bubble them up from the directives.
        scope.onBlur = function() {
          $timeout(onBlurNow);
        };

        scope.$watch('boxId', function() {
          var box = scope.workspace.boxMap[scope.boxId];
          if (!box) { return; }
          scope.metadata = {
            id: box.instance.id,
            operationId: box.instance.operationId,
          };
        });
      },
    };
  });
