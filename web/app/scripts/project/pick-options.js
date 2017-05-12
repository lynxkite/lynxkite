// The component responsible for center selection.
//
// The state of the picker is scattered around a few places. The state needs to be persisted
// for two reasons: page reloads, visualizations save/load.
//  scope.count   (persisted via scope.side.state.lastCentersRequest,
//                 see scope.updateFromLastCentersRequest())
//    - UI-bound value of number of centers requested
//  scope.filters   (persisted via scope.side.state.lastCentersRequest,
//                   see scope.updateFromLastCentersRequest())
//    - UI-bound value for filters.
//  scope.side.state.customVisualizationFilters  (persisted via state, see UIStatus.scala)
//    - UI-bound value of the toggle switch between custom or project restrictions.
//
//  scope.side.pickOptions.offset  (not persisted)
//    - Number of times "Pick"/"Next" button was pushed without changing
//      parameters.
//  scope.pickByOffsetMode (not persisted)
//    - UI bound boolean toggle. If true, then the user can see and modify
//      the offset in a text input field.
//  scope.editedOffset  (not persisted)
//    - A copy of scope.side.pickOptions.offset, bound to the UI. If this is changed by the
//      user and pickByOffsetMode is true, then the Pick/Next button will turn into a
//      "Pick by offset" button.
//  scope.side.pickOptions.lastCenterRequestParameters   (not persisted)
//    - last pick request generated from the UI. This is used to detect
//      changes of the parameters by the user on the UI and then to decide
//      whether to show a 'Pick' or a 'Next' button.
//
//  scope.side.state.lastCentersRequest   (persisted via state, see UIStatus.scala)
//    - The last successful centers request
//  scope.side.state.lastCentersResponse   (not persisted)
//    - The response to lastCentersRequest
//  scope.side.state.centers   (persisted via state, see UIStatus.scala)
//    - The centers from lastCentersRequest (or overridden by the user). UI-bound
//
// What happens when the user presses Pick/Next?
// 1. scope.side.pickOptions.lastCenterRequestParameters is updated if there was a change
//    on the UI.
// 2. The params are sent to backend via side.sendCenterRequest(). In case of success,
//    state.centers, state.lastCenterRequest and state.lastCentersResponse is updated.
// 3. This also triggers scope.updateFromLastCentersRequest(), but in theory, it should
//    not make any difference in this case.
//
// What happens when loading a visualization?
// 1. side.state.* is updated by the load.
// 2. scope.updateFromLastCentersRequest() is triggered and that updates the UI from
//    scope.side.state.lastCenterRequest
//
// What happens on a page reload?
// 1. The DOM tree and angular controls are constructed from zero.
// 2. side.state is restored from the URL
// 3. scope.updateFromLastCentersRequest() is triggered and that updates the UI from
//    scope.side.state.lastCenterRequest
//
//
// Have fun!

'use strict';

angular.module('biggraph').directive('pickOptions', function() {
  return {
    scope: { side: '=' },
    templateUrl: 'scripts/project/pick-options.html',
    link: function(scope) {
      scope.count = '1';
      scope.editedOffset = '';
      scope.filters = [];
      scope.side.pickOptions = scope.side.pickOptions || {};

      scope.updateFromLastCentersRequest = function() {
        // pickOptions is stored in "side" to survive even when the picker is recreated.
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

      scope.togglePickByOffsetMode = function() {
        if (!scope.pickByOffsetMode) {
          scope.editedOffset = scope.getCurrentPickOffset();
          scope.pickByOffsetMode = true;
        } else {
          scope.pickByOffsetMode = false;
        }
      };

      scope.pickByOffsetWasEdited = function() {
        if (!scope.pickByOffsetMode) {
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
        if (scope.pickByOffsetWasEdited()) {  // "Pick by offset"
          offset = parseInt(scope.editedOffset) || 0;
        } else if (unchanged) { // "Next"
          offset = scope.side.pickOptions.offset + params.count;
        } else {
          offset = 0;
        }
        // Configure and send request.
        if (unchanged) { // "Next" or "Pick by offset"
          scope.side.pickOptions.offset = offset;
          params.offset = scope.side.pickOptions.offset;
        } else { // "Pick" or "Pick by offset"
          scope.side.pickOptions = {
            offset: offset,
            lastCenterRequestParameters: scope.side.resolveCenterRequestParams(params),
          };
        }
        scope.side.sendCenterRequest(params);
        // Update "Pick by offset" input field value.
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

      scope.$watch('side.state.customVisualizationFilters', function(newValue, oldValue) {
        if (newValue !== oldValue) {
          scope.toggleCustomFilters(newValue);
        }
      });
      scope.$watch('side.state.lastCentersRequest', function() {
        scope.updateFromLastCentersRequest();
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
