'use strict';

// Viewer of an exportResult state.

angular.module('biggraph')
 .directive('exportResult', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/export-result.html',
      scope: {
        workspace: '=',
      },
      link: function(scope) {
         var exportResult = util.nocache(
                               'ajax/getExportResultOutput',
                               {
                                 stateId: scope.workspace.selectedStateId,
                               });
        exportResult.then(
          function success(exportResult) {
            scope.alreadyExported = exportResult.computeProgress;

            scope.prettifyCamelCase = function(camelCase) {
              // insert a space before all caps
              var split = camelCase.replace(/([A-Z])/g, ' $1');
              // uppercase the first character
              var prettified = split.replace(/^./, function(str) {
                return str.toUpperCase();
                });
              return prettified;
            };
            scope.fileMetaData =
              (exportResult.computedValue) ? JSON.parse(exportResult.computedValue.string) : {};

            scope.export = function() {
              util.lazyFetchScalarValue(exportResult, true);
            };
          });
      },
    };
});
