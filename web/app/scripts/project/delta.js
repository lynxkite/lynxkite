// A scalar that represents a change in another scalar.
'use strict';
import '../app';
import templateUrl from './delta.html?url';

angular.module('biggraph').directive('delta', function() {
  return {
    restrict: 'E',
    scope: { ref: '=' },
    templateUrl,
  };
});
