'use strict';

angular.module('biggraph').directive('lazySlider', function() {
  return {
    restrict: 'E',
    scope: { value: '=' },
    replace: false,
    templateUrl: 'lazy-slider.html',
    link: function(scope) {
      scope.$watch('value', update);

      function update() {
        scope.tmp = scope.value;
      }

      scope.set = function(newValue) {
        scope.value = newValue;
      };
    }
  };
});
