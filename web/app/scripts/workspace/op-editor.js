// Editor of operation parameters.
'use strict';
import '../app';

angular.module('biggraph')
  .directive('opEditor', function() {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/op-editor.template',
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
