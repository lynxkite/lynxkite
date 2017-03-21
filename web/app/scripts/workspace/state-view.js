'use strict';

// Viewer of a state at an output of a box.

angular.module('biggraph')
 .directive('stateView', function() {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/state-view.html',
      scope: {
        state: '='
      }
    };
});
