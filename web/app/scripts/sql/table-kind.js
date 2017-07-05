// UI for the "table" parameter kind.
'use strict';

angular.module('biggraph').directive('tableKind', function(util) {
  return {
    scope: {
      box: '=',
      params: '=',
      guid: '=',
      fileUploads: '=',
      onBlur: '&',
    },
    templateUrl: 'scripts/sql/table-kind.html',
    link: function(scope) {
      scope.importBox = function() {
        scope.disabled = true;
        scope.error = undefined;
        var box = angular.copy(scope.box.instance);
        box.parameters = scope.params;
        util.post('/ajax/importBox', box).then(function success(response) {
          scope.guid = response.guid;
          console.log('scope.params', scope.params);
          scope.params['last_hash'] = response.parameterHash;
          scope.onBlur();
        }, function error(error) {
          scope.error = error;
        }).finally(function() {
          scope.disabled = false;
        });
      };
    },
  };
});
