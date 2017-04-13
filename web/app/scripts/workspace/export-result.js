'use strict';

// Viewer of an exportResult state.

angular.module('biggraph')
 .directive('exportResult', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/export-result.html',
      scope: {
        workspace: '=',
        plug: '=',
      },
      link: function(scope) {
         var exportResult = util.nocache(
                               'ajax/getExternalFileOutput',
                               {
                                 workspace: scope.workspace,
                                 output: {
                                   boxID: scope.plug.boxId,
                                   id: scope.plug.id
                                 }
                               });
        exportResult.then(
          function success(exportResult) {
            scope.scalar = exportResult.scalar;
            scope.exported = exportResult.computed;
            scope.export = function() {
              util.lazyFetchScalarValue(scope.scalar, true);
            };
          });
      },
    };
});
