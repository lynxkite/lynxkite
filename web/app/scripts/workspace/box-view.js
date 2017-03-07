'use strict';

// Viewer and editor of a box instance.

angular.module('biggraph')
 .directive('boxView', function() {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/box-view.html',
      scope: {
        box: '='
      }
    };
});
