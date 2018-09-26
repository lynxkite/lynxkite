'use strict';

// Viewer of a table snapshot in the entry selector.

angular.module('biggraph')
  .directive('snapshotViewer', function(util) {
    return {
      restrict: 'E',
      template: `<table-state-view ng-if="stateId" state-id="stateId">
                 </table-state-view>`,
      scope: {
        path: '@',
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
          });
        const req = scope.result;

        req.then(function(res) {
          console.log(res);
          scope.data = res;
          scope.stateId = scope.data.outputs[0].stateId;
        });


      },
    };
  });
