// UI for the "trigger" parameter kind.
'use strict';
import '../app';
import '../util/util';
import templateUrl from './trigger-parameter.html?url';

angular.module('biggraph').directive('triggerParameter', function(util) {
  return {
    scope: {
      box: '=',
      param: '=',
    },
    templateUrl,
    link: function(scope) {
      scope.disabled = false;
      scope.computed = false;
      scope.trigger = function() {
        scope.disabled = true;
        scope.computed = false;
        scope.error = undefined;
        util.post('/ajax/triggerBox', {
          workspace: scope.box.workspace.ref(),
          box: scope.box.instance.id,
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
