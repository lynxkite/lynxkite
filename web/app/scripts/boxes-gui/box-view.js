'use strict';

angular.module('biggraph')
 .directive('boxView', function() {
    return {
      restrict: 'E',
      templateUrl: 'scripts/boxes-gui/box-view.html',
      scope: {
        box: '='
      }
    };
});
