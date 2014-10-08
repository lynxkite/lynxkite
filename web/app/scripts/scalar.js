'use strict';

angular.module('biggraph').directive('scalar', function() {
  return {
    scope: { scalar: '=', value: '@', side: '=' },
    templateUrl: 'scalar.html',
  };
});
