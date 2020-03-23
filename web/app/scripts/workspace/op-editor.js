// Editor of operation parameters.
'use strict';

angular.module('biggraph')
  .directive('opEditor', function() {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/op-editor.html',
      scope: {
        box: '=?',
        boxMeta: '=',
        parameters: '=',
        parametricParameters: '=',
        workspace: '=',
        wizard: '=',
        halfSize: '=?',
        onBlur: '&?',
      },
    };
  });
