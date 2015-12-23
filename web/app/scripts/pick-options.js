// The component responsible for center selection.
'use strict';

angular.module('biggraph').directive('pickOptions', function() {
  return {
    scope: { side: '=' },
    templateUrl: 'pick-options.html',
    link: function(scope) {
      scope.count = '1';
      scope.editedOffset = '';
      scope.reset = function() {
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
        if (scope.side.state.customVisualizationFilters) {
          var filters = scope.filters.filter(function (filter) {
            return filter.attributeName !== '';
          });
          return {
            count: parseInt(scope.count),
            filters: filters };
        } else {
          return {
            count: parseInt(scope.count),
            filters: scope.side.nonEmptyVertexFilterNames() };
        }
      }

      scope.unchanged = function() {
        var resolvedParams = scope.side.resolveCenterRequestParams(centerRequestParams());
        return angular.equals(scope.side.pickOptions.lastCenterRequestParameters, resolvedParams);
      };

      scope.getCurrentPickOffset = function() {
        return (scope.side.pickOptions.offset || 0).toString();
      };

      scope.togglePickByIdMode = function() {
        if (!scope.pickByIdMode) {
          scope.editedOffset = scope.getCurrentPickOffset();
          scope.pickByIdMode = true;
        } else {
          scope.pickByIdMode = false;
        }
      };

      scope.pickByIdWasEdited = function() {
        if (!scope.pickByIdMode) {
          return false;
        }
        if (scope.unchanged()) {
          return scope.editedOffset !== scope.getCurrentPickOffset();
        } else {
          return scope.editedOffset !== '0';
        }
      };

      scope.requestNewCenters = function() {
        var unchanged = scope.unchanged();
        var params = centerRequestParams();
        var offset = 0;
        // Compute offset.
        if (scope.pickByIdWasEdited()) {  // "Pick by #ID"
          offset = parseInt(scope.editedOffset) || 0;
        } else if (unchanged) { // "Next"
          offset = scope.side.pickOptions.offset + params.count;
        } else {
          offset = 0;
        }
        // Configure and send request.
        if (unchanged) { // "Next" or "Pick by #ID"
          scope.side.pickOptions.offset = offset;
          params.offset = scope.side.pickOptions.offset;
        } else { // "Pick" or "Pick by #ID"
          scope.side.pickOptions = {
            offset: offset,
            lastCenterRequestParameters: scope.side.resolveCenterRequestParams(params),
          };
        }
        scope.side.sendCenterRequest(params);
        // Update "Pick by #ID" input field value.
        scope.editedOffset = scope.getCurrentPickOffset();
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

      scope.toggleCustomFilters = function(custom) {
        if (custom) {
          scope.copyRestrictionsFromFilters();
        } else {
          scope.filters = [];
        }
      };

      scope.$watch('side.state.lastCentersRequest', function() {
        scope.reset();
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
