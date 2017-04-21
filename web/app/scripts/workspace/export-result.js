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
          scope.exportResult = util.get(
                               'ajax/getExportResultOutput',
                               {
                                 stateId: scope.stateId,
                               });
          scope.exportResult.then(function success(exportResult) {
            if (exportResult.computeProgress === 0.5) {
              // This creates the scalarVale so that the progress bar shows up even if you
              // have started the export before clicking on the plug.
              scope.export();
            } else {
                scope.alreadyExported = (exportResult.computeProgress === 1) ? true : false;
                scope.error =
                    (exportResult.computeProgress === -1) ? exportResult.errorMessage : undefined;
                var metaData =
                  (exportResult.computedValue) ? JSON.parse(exportResult.computedValue.string)
                                           : undefined;
                scope.metaDataToDisplay = new CollectedMetaDataToDisplay(metaData);
            }
          });
        });

        scope.export = function() {
          scope.scalarValue = util.lazyFetchScalarValue(scope.exportResult, true);
          scope.scalarValue.value.then(function success(result) {
             scope.alreadyExported = true;
            var metaData = JSON.parse(result.string);
            if(metaData.download) {
              $window.location =
                    '/downloadFile?q=' + encodeURIComponent(JSON.stringify(metaData.download));
            }
            scope.metaDataToDisplay = new CollectedMetaDataToDisplay(metaData);
          }, function error(error) {
               scope.error = error.data;
               util.ajaxError(error);
          }).finally(function() {
          });
        };

        function CollectedMetaDataToDisplay(metaData) {
          if (metaData) {
            var fEMetaData = {};
            if (metaData.file) {
              fEMetaData.Format =  metaData.file.format;
              if (metaData.download) {
                var splitPath = metaData.download.path.split('/');
                fEMetaData['Downloaded as'] = splitPath[splitPath.length - 1];
              } else {
                  fEMetaData.Path = metaData.file.path;
              }
            } else if (metaData.jdbc) {
                fEMetaData['JDBC URL'] = metaData.jdbc.jdbcUrl;
                fEMetaData.Table = metaData.jdbc.table;
                fEMetaData.mode = metaData.jdbc.mode;
           }
            return fEMetaData;
          } else {
              return {};
          }
        }
      },
    };
});
