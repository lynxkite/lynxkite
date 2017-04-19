'use strict';

// Viewer of an exportResult state.

angular.module('biggraph')
 .directive('exportResult', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/export-result.html',
      scope: {
        stateId: '=',
      },
      link: function(scope) {
        util.deepWatch(scope, 'stateId', function() {
         scope.exportResult = util.nocache(
                               'ajax/getExportResultOutput',
                               {
                                 stateId: scope.stateId,
                               });

         scope.exportResult.then(function success(exportResult) {
           scope.alreadyExported = (exportResult.computeProgress === 1.0) ? true : false;
           scope.fileMetaData =
             (exportResult.computedValue) ? JSON.parse(exportResult.computedValue.string) : {};
             });
        });

        scope.export = function() {
            var scalarValue = util.lazyFetchScalarValue(scope.exportResult, true);
            scope.alreadyExported = true;
            scalarValue.value.then(function success(value) {
              scope.fileMetaData = JSON.parse(value.string);
            });

          };

        scope.prettifyCamelCase = function(camelCase) {
          // insert a space before all caps
          var split = camelCase.replace(/([A-Z])/g, ' $1');
          // uppercase the first character
          var prettified = split.replace(/^./, function(str) {
            return str.toUpperCase();
            });
          return prettified;
        };
      },
    };
});
