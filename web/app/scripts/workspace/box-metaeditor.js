// Box metadata editor.
'use strict';
import '../app';
import templateUrl from './box-metaeditor.html?url';

angular.module('biggraph')
  .directive('boxMetaeditor', function($timeout) {
    return {
      restrict: 'E',
      templateUrl,
      scope: {
        workspace: '=',
        boxId: '=',
        popupModel: '=',
      },
      link: function(scope, element) {
        element.on('focusout', function() { scope.onBlur(); });

        function onBlurNow() {
          const box = scope.workspace.boxMap[scope.boxId];
          let changed = false;
          if (box.instance.operationId !== scope.metadata.operationId) {
            changed = true;
            box.instance.operationId = scope.metadata.operationId;
          }
          if (box.instance.id !== scope.metadata.id) {
            changed = true;
            const newId = scope.metadata.id;
            box.id = newId;
            box.instance.id = newId;
            // Update connections.
            for (let i = 0; i < box.outputs.length; ++i) {
              const src = box.outputs[i];
              const dsts = src.getAttachedPlugs();
              for (let j = 0; j < dsts.length; ++j) {
                const dst = dsts[j];
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
          const box = scope.workspace.boxMap[scope.boxId];
          if (!box) { return; }
          scope.metadata = {
            id: box.instance.id,
            operationId: box.instance.operationId,
          };
        });
      },
    };
  });
