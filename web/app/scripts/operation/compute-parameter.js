// UI for the "compute" parameter kind.
'use strict';

angular.module('biggraph').directive('computeParameter', function(util) {
  return {
    scope: {
      box: '=',
    },
    templateUrl: 'scripts/operation/compute-parameter.html',
    link: function(scope) {
      scope.disabled = false;
      scope.computed = false;
      scope.compute = function() {
        scope.disabled = true;
        scope.computed = false;
        scope.error = undefined;
        var box = angular.copy(scope.box.instance);
        util.get('/ajax/getComputeBoxResult', {
          box: box,
          ref: scope.box.workspace.ref(),
        }).then(function success() {
          scope.computed = true;
        }, function error(error) {
          scope.error = error;
        }).finally(function() {
          scope.disabled = false;
        });
      };
    },
  };
});
