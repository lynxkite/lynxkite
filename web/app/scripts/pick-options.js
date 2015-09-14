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
          var amount = 1;
          while (amount < scope.offset + count) {
            amount *= 10; // Fetch 10 examples, then 100, then 1000...
          }
          params.count = amount;
          scope.side.sendCenterRequest(params).then(function success() {
            var centers = scope.side.state.centers;
            var offset = scope.offset % centers.length;
            if (centers.length <= count) {
              // Use them all.
            } else if (centers.length < offset + count) {
              // Turn around.
              centers = centers.slice(offset).concat(centers).slice(0, count);
            } else {
              // Normal case.
              centers = centers.slice(offset, offset + count);
            }
            scope.side.state.centers = centers;
          });
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
