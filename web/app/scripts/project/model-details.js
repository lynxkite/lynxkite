// The entry for a model in the project view.
'use strict';

angular.module('biggraph').directive('modelDetails', function(util) {
  return {
    restrict: 'E',
    scope: { scalarId: '=' },
    templateUrl: 'scripts/project/model-details.html',
    link: function(scope) {
      scope.model = util.get('/ajax/model', {
        scalarId: scope.scalarId,
      });

      scope.reportError = function() {
        util.reportRequestError(scope.model, 'Error loading model.');
      };
    },
  };
});
