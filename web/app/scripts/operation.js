'use strict';

angular.module('biggraph').directive('operation', function () {
  return {
    restrict: 'E',
    scope: { op: '=model' },
    replace: false,
    templateUrl: 'operation.html',
  };
});
