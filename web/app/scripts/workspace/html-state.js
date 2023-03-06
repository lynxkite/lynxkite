'use strict';
import '../app';
import '../util/util';
import templateUrl from './html-state.html?url';

// Viewer of an HTML state.
angular.module('biggraph')
  .directive('htmlState', ["util", function(util) {
    return {
      restrict: 'E',
      templateUrl,
      scope: {
        stateId: '=',
      },
      link: function(scope) {
        util.deepWatch(scope, 'stateId', function() {
          scope.content = util.get('/ajax/scalarValue', {scalarId: scope.stateId});
        });
      },
    };
  }]);
