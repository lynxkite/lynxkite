// UI for the "hash-check" parameter kind.
'use strict';

angular.module('biggraph').directive('stalenessCheck', function(util) {
  return {
    scope: {
      box: '=',
      param: '=',
      onBlur: '&',
    },
    templateUrl: 'scripts/operation/staleness-check.html',
    link: function(scope) {
      util.deepWatch(scope, 'box.instance', function() {
        scope.onBlur();
        scope.message = scope.param.payload.stale ? 'stale!' : 'up to date';
      });
    }
  };
});
