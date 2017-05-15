// A small text box that can pop open inside a button.
// Can be used to provide a name when forking, for example.
'use strict';

angular.module('biggraph').directive('inlineInput', function($q) {
  return {
    restrict: 'E',
    scope: { onsubmit: '&', placeholder: '@', open: '=' },
    templateUrl: 'scripts/util/inline-input.html',
    link: function(scope) {
      scope.enabled = true;
      scope.done = function(input) {
        scope.enabled = false;
        var promise = $q(scope.onsubmit({ input: input }));
        promise.then(scope.close);
      };

      scope.close = function() {
        scope.enabled = true;
        scope.open = false;
      };
    },
  };
});
