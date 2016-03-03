// A scalar that represents a change in another scalar.
'use strict';

angular.module('biggraph').directive('delta', function(util) {
  return {
    restrict: 'E',
    scope: { ref: '=', tip: '=' },
    templateUrl: 'delta.html',
  };
});
