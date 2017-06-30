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
      link: function(scope, element) {
        scope.popupModel.element = element;
        var observer = new MutationObserver(function() {
          scope.$apply(function() {
            scope.popupModel.updateSize();
          });
        });
        observer.observe(element.find('.popup-content')[0], { attributes: true });
        scope.$on('$destroy', function() {
          observer.disconnect();
        });
      },
    };
  });
