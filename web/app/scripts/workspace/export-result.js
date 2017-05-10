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
         scope.exportResultOutput = util.get(
                               'ajax/getExportResultOutput',
           {
             stateId: scope.stateId,
           }
         );
         scope.exportResultOutput.then(function success(exportResultOutput) {
           scope.result = exportResultOutput.result;
         });
       });

       util.deepWatch(scope, 'result', function() {
         var progress = scope.result.computeProgress;
         scope.alreadyExported = (progress === 1) ? true : false;
         scope.error = (progress === -1) ? scope.result.errorMessage : undefined;
       });

       scope.export = function() {
         util.lazyFetchScalarValue(scope.result, true);
       };

       scope.download = function () {
         $window.location =
               '/downloadFile?q=' + encodeURIComponent(JSON.stringify(
                 {
                   path: scope.parameters.path,
                   stripHeaders: false
                 }));
       };
     },
   };
 });
