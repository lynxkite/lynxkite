'use strict';

angular.module('biggraph').directive('scalar', function(util) {
  return {
    scope: { scalar: '=', side: '=' },
    templateUrl: 'scalar.html',
    link: function(scope) {
      util.deepWatch(scope, 'scalar', function() {
        scope.value = util.get('/ajax/scalarValue', { scalarId: scope.scalar.id });
      });
    }
  };
});
