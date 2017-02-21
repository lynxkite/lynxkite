'use strict';

angular.module('biggraph')
 .directive('projectStateView', function() {
    return {
      restrict: 'E',
      templateUrl: 'scripts/boxes-gui/project-state-view.html',
      scope: {
        side: '='
      },
    };
});
