'use strict';

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
