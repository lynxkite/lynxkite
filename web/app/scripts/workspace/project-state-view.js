'use strict';

// Viewer of a project state.
// This is like the project view of the old LynxKite UI.
//
// The current implementation is just a subset copy of side.js and many features are
// missing.

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
