// The UI for advanced center selection in sampled mode.
'use strict';

angular.module('biggraph').directive('pickOptions', function() {
  return {
    scope: { side: '=', shown: '=' },
    templateUrl: 'pick-options.html',
    link: function(scope) {
      scope.reset = function() {
        scope.count = '1';
        scope.filters = [];
        var lastCentersRequest = scope.side.state.lastCentersRequest;
        if (lastCentersRequest) {
          if (lastCentersRequest.filters) {
            scope.filters = lastCentersRequest.filters;
          }
          if (lastCentersRequest.count) {
            scope.count = lastCentersRequest.count.toString();
          }
        }
      };
      scope.copyRestrictionsFromFilters = function() {
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
