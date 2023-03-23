// A slider (input[type=range]) that only updates the model when the mouse is released.
import '../app';
import templateUrl from './lazy-slider.html?url';

angular.module('biggraph').directive('lazySlider', function() {
  return {
    restrict: 'E',
    scope: { value: '=' },
    replace: false,
    templateUrl,
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
