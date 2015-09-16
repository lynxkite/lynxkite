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
      scope.reset();

      scope.copyRestrictionsFromFilters = function() {
        scope.filters = scope.side.nonEmptyVertexFilterNames();
      };

      function centerRequestParams() {
        var filters = scope.filters.filter(function (filter) {
          return filter.attributeName !== '';
        });
        return {
          count: parseInt(scope.count),
          filters: filters };
      }

      scope.unchanged = function() {
        var resolvedParams = scope.side.resolveCenterRequestParams(centerRequestParams());
        return angular.equals(scope.lastCenterRequestParameters, resolvedParams);
      };

      scope.requestNewCenters = function() {
        var params = centerRequestParams();
        if (scope.unchanged()) { // "Next"
          var count = params.count;
          scope.offset += count;
          scope.side.sendCenterRequest(params, scope.offset);
        } else { // "Pick"
          scope.offset = 0;
          scope.side.sendCenterRequest(params);
          scope.lastCenterRequestParameters = scope.side.resolveCenterRequestParams(params);
        }
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
