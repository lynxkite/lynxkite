'use strict';

angular.module('biggraph').directive('expander', function() {
  return {
    restrict: 'E',
    scope: { show: '=model', help: '@' },
    replace: true,
    templateUrl: 'expander.html',
  };
});
