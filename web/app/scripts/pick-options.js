// The component responsible for center selection.
'use strict';

angular.module('biggraph').directive('pickOptions', function() {
  return {
    scope: { side: '=' },
    templateUrl: 'pick-options.html',
    link: function(scope) {
      scope.reset = function() {
        scope.count = '1';
        scope.filters = [];
        // pickOptions is stored in "side" to survive even when the picker is recreated.
        scope.side.pickOptions = scope.side.pickOptions || {};
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
          return {
            count: 1,
            filters: scope.side.nonEmptyVertexFilterNames() };
        }
      }

      scope.unchanged = function() {
        var resolvedParams = scope.side.resolveCenterRequestParams(centerRequestParams());
        return angular.equals(scope.side.pickOptions.lastCenterRequestParameters, resolvedParams);
      };

      scope.requestNewCenters = function() {
        var params = centerRequestParams();
        if (scope.unchanged()) { // "Next"
          scope.side.pickOptions.offset += params.count;
          params.offset = scope.side.pickOptions.offset;
        } else { // "Pick"
          scope.side.pickOptions = {
            offset: 0,
            lastCenterRequestParameters: scope.side.resolveCenterRequestParams(params),
          };
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

      scope.$watch('side.state.graphMode', function() {
        var state = scope.side.state;
        if (state.graphMode === 'sampled' && state.centers === undefined) {
          scope.requestNewCenters();
        }
      });
    },
  };
});
