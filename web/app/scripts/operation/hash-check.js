// UI for the "hash-check" parameter kind.
'use strict';

angular.module('biggraph').directive('hashCheck', function() {
  return {
    scope: {
      box: '=',
      params: '=',
      hash: '=',
      onBlur: '&',
    },
    templateUrl: 'scripts/operation/hash-check.html',
    link: function(scope) {
      console.log(scope);
    }
  };
});
