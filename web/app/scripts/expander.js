'use strict';

angular.module('biggraph').directive('expander', function($resource) {
  return {
    restrict: 'E',
    scope: { show: '=model', help: '@' },
    replace: true,
    templateUrl: 'expander.html',
  };
});
