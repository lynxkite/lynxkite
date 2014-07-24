'use strict';

angular.module('biggraph').directive('operations', function () {
  return {
    restrict: 'E',
    scope: { ops: '=model' },
    replace: true,
    templateUrl: 'operations.html',
    link: function(scope) {
    },
  };
});
