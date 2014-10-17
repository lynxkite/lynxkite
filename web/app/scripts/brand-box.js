'use strict';

angular.module('biggraph').directive('brandBox', function() {
  return {
    restrict: 'E',
    templateUrl: 'brand-box.html',
    link: function(scope) {
      var tips = [
        'You can zoom the graph visualization with the mouse wheel.',
        'Press ? for a list of keyboard shortcuts.',
        'The system is busy when you see the gears turning in the bottom right corner.' +
          ' Hover over the gears for the option to abort the calculation.',
        'Click on approximate numbers like 42M to see the exact value.',
        'Apply custom colors: gender == \'female\' ? \'pink\' : \'lightblue\'',
        'Click on a histogram bar to zoom in.',
        'Multiple monitors? Enable linked mode at the bottom of the page.',
        'Press / to quickly access operations by their name.',
      ];
      scope.tip = tips[Math.floor(Math.random() * tips.length)];
    },
  };
});
