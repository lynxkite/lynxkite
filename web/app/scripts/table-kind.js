// UI for the "table" parameter kind.
'use strict';

angular.module('biggraph').directive('tableKind', function(util) {
  return {
    scope: {
      box: '=',
      params: '=',
      snapshot: '=',
      fileUploads: '=',
    },
    templateUrl: 'table-kind.html',
    link: function(scope) {
      scope.createSnapshot = function() {
        scope.disabled = true;
        scope.error = undefined;
        var box = angular.copy(scope.box.instance);
        box.parameters = scope.params;
        util.post('/ajax/importBox', box).then(function success(response) {
          scope.snapshot = JSON.stringify(response);
        }, function error(error) {
          scope.error = error;
        }).finally(function() {
          scope.disabled = false;
        });
      };
    },
  };
});
