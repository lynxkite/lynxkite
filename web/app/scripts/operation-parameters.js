'use strict';

angular.module('biggraph').directive('operationParameters', function() {
  return {
    restrict: 'E',
    scope: { input: '=', output: '=', busy: '=?' },
    templateUrl: 'operation-parameters.html',
    link: function(scope) {
      scope.fileUploads = { count: 0 };
      scope.$watch('fileUploads.count', function(count) {
        scope.busy = count !== 0;
      });
    }
  };
});
