// The "brand box" is displayed on the right hand side when only one project is open.
// It includes the Lynx Analytics logo and randomly chosen tips.
'use strict';

angular.module('biggraph').directive('brandBox', function() {
  return {
    restrict: 'E',
    templateUrl: 'brand-box.html',
    link: function(scope) {
      var tips = [
        'You can zoom the graph visualization with the mouse wheel or by (right)' +
          ' double-clicking.',
        'Press ? for a list of keyboard shortcuts.',
        'The system is busy when you see the gears turning in the bottom right corner.' +
          ' Hover over the gears for the option to abort the calculation.',
        'Click on approximate numbers like 42M to see the exact value.',
        'Apply custom colors by creating an attribute with the color names.' +
          ' Use the "Derive attribute" operation:' +
          ' <tt>gender == \'female\' ? \'pink\' : \'lightblue\'</tt>',
        'Click on a histogram bar to zoom in.',
        'Multiple monitors? Enable linked mode at the bottom of the page.',
        'Press / to quickly access operations by their name.',
        'Click on a vertex to open the context menu.',
        'Open the same project on both sides to graph edges between different views of the graph.',
        'You can copy histogram and graph data to the clipboard with the' +
          ' <i class="glyphicon glyphicon-th"></i> buttons.',
        'Shift-scroll to adjust vertex and edge sizes without zooming.',
        'Drag the graph background to pan the view.',
        'Click the handle on the left of the graph visualization for extra options.',
        'Close the open sub-segmentation of a segmentation to reopen the parent project.'
      ];
      scope.tip = tips[Math.floor(Math.random() * tips.length)];
    },
  };
});
