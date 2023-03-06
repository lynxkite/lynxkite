'use strict';
import '../app';
import '../util/util';

// Viewer of an HTML state.
angular.module('biggraph')
  .directive('htmlState', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/html-state.html',
      scope: {
        stateId: '=',
      },
      link: function(scope) {
        util.deepWatch(scope, 'stateId', function() {
          scope.content = util.get('/ajax/scalarValue', {scalarId: scope.stateId});
        });
      },
    };
  });
