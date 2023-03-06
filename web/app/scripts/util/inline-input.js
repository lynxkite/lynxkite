// A small text box that can pop open inside a button.
// Can be used to provide a name when forking, for example.
'use strict';
import '../app';

angular.module('biggraph').directive('inlineInput', function() {
  return {
    restrict: 'E',
    scope: {
      onsubmit: '&',
      input: '=?',
      placeholder: '@',
      open: '=',
    },
    templateUrl: 'scripts/util/inline-input.template',
    link: function(scope, element) {
      scope.enabled = true;
      scope.done = function() {
        scope.enabled = false;
        scope.onsubmit({
          input: scope.input || '',
          done: () => scope.enabled = true,
        });
      };

      scope.close = function() {
        scope.input = '';
        scope.enabled = true;
        scope.open = false;
      };

      scope.$watch('open', function(open) {
        if (open) { element.find('input').focus(); }
      });
    },
  };
});
