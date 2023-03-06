// UI for the "table" parameter kind.
'use strict';
import '../app';
import '../util/util';

angular.module('biggraph').directive('tableKind', function(util) {
  return {
    scope: {
      box: '=',
      params: '=',
      guid: '=',
      workspaceReference: '&',
      onBlur: '&',
    },
    templateUrl: 'scripts/sql/table-kind.html',
    link: function(scope) {
      scope.importBox = function() {
        scope.inProgress = true;
        const box = angular.copy(scope.box.instance);
        box.parameters = scope.params;
        util.post('/ajax/importBox', {
          box: box,
          ref: scope.workspaceReference(),
        }).then(function success(response) {
          scope.guid = response.guid;
          scope.params.last_settings = response.parameterSettings;
          scope.onBlur();
        }).finally(function() {
          scope.inProgress = false;
        });
      };
      scope.stale = function() {
        const params = angular.copy(scope.params);
        delete params.last_settings;
        delete params.imported_table;
        const fresh = scope.params.last_settings &&
          angular.equals(params, JSON.parse(scope.params.last_settings));
        return !fresh;
      };
      scope.tooltip = function() {
        if (scope.inProgress) {
          return '';
        } else if (scope.stale()) {
          return 'The parameters have changed. Please run the import.';
        } else {
          return `
            The parameters have not changed since the last import.
            You can re-run the import if the datasource has changed.`;
        }
      };
    },
  };
});
