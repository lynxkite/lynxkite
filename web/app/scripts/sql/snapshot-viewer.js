'use strict';

// Viewer of a table snapshot in the entry selector.

angular.module('biggraph')
  .directive('snapshotViewer', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/sql/snapshot-viewer.html',
      scope: {
        path: '@',
        type: '@',
      },
      link: function(scope) {
        scope.result = util.post( // dummy workspace to create a state
          '/ajax/runWorkspace',
          {workspace: {
            boxes: [
              {
                id: 'anchor',
                operationId: 'Anchor',
                parameters: {},
                x: 0, y: 0,
                inputs: {},
                parametricParameters: {},
              },
              {
                id: 'box_0',
                operationId: 'Import snapshot',
                parameters: {path: scope.path},
                x: 0, y: 0,
                inputs: {},
                parametricParameters: {}}
            ]},
            parameters: {},
          }).then(function(res) {
            scope.data = res;
            scope.stateId = scope.data.outputs[0].stateId;
            // Fake context for general state viewer
            scope.popupModel = {};
            scope.popupModel.width = 500;
            scope.popupModel.height = 500;
            scope.popupModel.maxHeight = 500;
            scope.plug = {};
            scope.plug.stateId = scope.stateId;
            scope.plug.kind = scope.type;
          });
      },
    };
  });
