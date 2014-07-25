'use strict';

angular.module('biggraph').directive('operationToolbox', function($resource) {
  return {
    restrict: 'E',
    scope: { side: '=' },
    replace: true,
    templateUrl: 'operation-toolbox.html',
    link: function(scope) {
      var ApplyOp = $resource('/ajax/applyOp');
      scope.categories = [
        { icon: 'A', color: 'yellow', title: 'Operations for attributes',
          ops: scope.side.data.attributeOps },
        { icon: 'E', color: 'orange', title: 'Operations for ' + scope.side.data.edgeNames,
          ops: scope.side.data.edgeOps },
        { icon: 'V', color: 'green', title: 'Operations for ' + scope.side.data.vertexNames,
          ops: scope.side.data.vertexOps },
        { icon: 'S', color: 'blue', title: 'Operations for segmentations',
          ops: scope.side.data.segmentationOps },
      ];
      function close() {
        scope.active = undefined;
        scope.ops = undefined;
      }
      function open(cat) {
        scope.active = cat;
        scope.ops = cat.ops;
      }
      scope.clicked = function(cat) {
        if (scope.active === cat) {
          close();
        } else {
          open(cat);
        }
      };
    },
  };
});
