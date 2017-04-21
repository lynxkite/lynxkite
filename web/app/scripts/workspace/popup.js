'use strict';

// A movable popup window that lives on the drawing board.
// It can contain either a box editor or a state viewer.

angular.module('biggraph')
  .directive('popup', function() {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/popup.html',
      scope: {
        popupModel: '=',
        workspace: '=',
      },
    };
  });
