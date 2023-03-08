// Editor of operation parameters.
import '../app';
import templateUrl from './op-editor.html?url';

angular.module('biggraph')
  .directive('opEditor', function() {
    return {
      restrict: 'E',
      templateUrl,
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
