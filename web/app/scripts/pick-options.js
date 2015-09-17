// The UI for advanced center selection in sampled mode.
'use strict';

angular.module('biggraph').directive('pickOptions', function() {
  return {
    scope: { side: '=' },
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
      scope.reset();

      scope.copyRestrictionsFromFilters = function() {
        scope.filters = scope.side.nonEmptyVertexFilterNames();
      };

      function centerRequestParams() {
        if (scope.advanced) {
          var filters = scope.filters.filter(function (filter) {
            return filter.attributeName !== '';
          });
          return {
            count: parseInt(scope.count),
            filters: filters };
        } else {
          return { count: 1, filters: [] };
        }
      }

      scope.unchanged = function() {
        var resolvedParams = scope.side.resolveCenterRequestParams(centerRequestParams());
        return angular.equals(scope.lastCenterRequestParameters, resolvedParams);
      };

      scope.requestNewCenters = function() {
        var params = centerRequestParams();
        if (scope.unchanged()) { // "Next"
          scope.offset += params.count;
          params.offset = scope.offset;
        } else { // "Pick"
          scope.offset = 0;
          scope.lastCenterRequestParameters = scope.side.resolveCenterRequestParams(params);
        }
        scope.side.sendCenterRequest(params);
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

      scope.$watch('advanced', function(advanced) {
        if (advanced) {
          scope.reset();
        }
      });
    },
  };
});
