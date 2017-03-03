'use strict';

angular.module('biggraph')
 .directive('projectStateView', function() {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/project-state-view.html',
      scope: {
        side: '='
      },
    };
});
