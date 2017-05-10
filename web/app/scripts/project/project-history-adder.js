// The toolbox shows the list of operation categories and the operations.
'use strict';

angular.module('biggraph')
  .directive('projectHistoryAdder', function() {
    return {
      restrict: 'E',
      // A lot of internals are exposed, because this directive is used both in
      // side-operation-toolbox and in project-history.
      scope: {
        segmentations: '=',  // (Input) List of possible segmentations.
        insertOperation: '&',  // (Method) The directive will call this to insert a new operation.
      },
      templateUrl: 'project-history-adder.html',
    };
  });
