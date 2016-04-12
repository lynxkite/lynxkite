// The toolbox shows the list of operation categories and the operations.
'use strict';

angular.module('biggraph')
  .directive('projectHistoryAdder', function() {
  return {
    restrict: 'E',
    // A lot of internals are exposed, because this directive is used both in
    // side-operation-toolbox and in project-history.
    scope: {
      step: '=',
      before: '=',
      historyScope: '='
    },
    templateUrl: 'project-history-adder.html',
    link: function(scope) {
      scope.insertOp = function(segmentation) {
        if (scope.before) {
          scope.historyScope.insertBefore(scope.step, segmentation);
        } else {
          scope.historyScope.insertAfter(scope.step, segmentation);
        }
      };
      scope.segmentations = function() {
        if (scope.before) {
          return scope.step.segmentationsBefore;
        } else {
          return scope.step.segmentationsAfter;
        }
      };
    }
  };
});
