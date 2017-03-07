'use strict';

// Viewer of a project state.
// This is like the project view of the old LynxKite UI.

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
