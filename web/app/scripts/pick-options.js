'use strict';

angular.module('biggraph').directive('pickOptions', function() {
  return {
    scope: { side: '=', shown: '=' },
    templateUrl: 'pick-options.html',
    link: function(scope) {
      scope.reset = function() {
        scope.count = '1';
        scope.filters = scope.side.nonEmptyVertexFilterNames();
      };
      scope.requestNewCenters = function() {
        scope.side.requestNewCentersWithFilters(
          parseInt(scope.count),
          scope.filters.filter(function (filter) { return filter.attributeName !== ''; }));
      };
      scope.addFilter = function() {
        scope.filters.push({
          attributeName: '',
          valueSpec: '',
        });
      };
      scope.removeFilter = function(idx) {
        scope.filters.splice(idx, 1);
      };
        
      scope.$watch('shown', function(shown) {
        if (shown) {
          scope.reset();
        }
      });
    },
  };
});
