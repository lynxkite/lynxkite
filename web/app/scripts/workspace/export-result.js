'use strict';
import '../app';
import '../util/util';
import templateUrl from './export-result.html?url';

// Viewer of an exportResult state.

angular.module('biggraph')
  .directive('exportResult', ["util", function(util) {
    return {
      restrict: 'E',
      templateUrl,
      scope: {
        stateId: '=',
      },
      link: function(scope) {
        util.deepWatch(scope, 'stateId', function() {
          scope.exportResultOutput = util.get(
            'ajax/getExportResultOutput',
            {
              stateId: scope.stateId,
            }
          );
          scope.exportResultOutput.then(function success(exportResultOutput) {
            // scope.status is the scalar value which is shown on the state viewer.
            scope.status = util.lazyFetchScalarValue(scope.exportResultOutput.result, false);
            // computeOnCommand is used in inline-loading. It makes it so that if the scalar
            // is not computed yet, it will show a Start computation button instead of the retry
            // button.
            scope.status.value.displayComputeButton = true;
            scope.parameters = exportResultOutput.parameters;
            // Only exported files can be downloaded, JDBC exports not.
            //
            scope.downloadable = (scope.parameters.format !== 'jdbc' && scope.parameters.for_download === 'yes') ? true : false;
          });
        });

        scope.downloadLink = function () {
          return 'downloadFile?q=' + encodeURIComponent(JSON.stringify(
            {
              path: scope.parameters.path,
              stripHeaders: scope.parameters.header === 'yes',
            }));
        };
      },
    };
  }]);
