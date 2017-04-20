'use strict';

// Viewer of an exportResult state.

angular.module('biggraph')
 .directive('exportResult', function(util, $window) {
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
            scope.alreadyExported = (exportResult.computeProgress === 1) ? true : false;
            scope.error =
              (exportResult.computeProgress === -1) ? exportResult.errorMessage : undefined;
            var metaData =
              (exportResult.computedValue) ? JSON.parse(exportResult.computedValue.string)
                                           : undefined;
            scope.fileMetaData = fileMetaDataToFE(metaData);
          });
        });

        scope.export = function() {
          var scalarValue = util.lazyFetchScalarValue(scope.exportResult, true);
          scalarValue.value.then(function success(result) {
            var metaData = JSON.parse(result.string);
            if(metaData.download) {
              $window.location =
                    '/downloadFile?q=' + encodeURIComponent(JSON.stringify(metaData.download));
            }
            scope.fileMetaData = fileMetaDataToFE(metaData);
          }, function error(error) {
               scope.error = error;
          }).finally(function() {
               scope.alreadyExported = true;
          });
        };

        var fileMetaDataToFE = function(metaData) {
          if (metaData) {
            var fEMetaData = {Format: metaData.format};
            if (metaData.download) {
              var splitPath = metaData.download.path.split('/');
              fEMetaData['Downloaded as'] = splitPath[splitPath.length - 1];
            } else {
                fEMetaData.Path = metaData.path;
            }
            return fEMetaData;
          } else {
              return {};
          }
        };
      },
    };
});
