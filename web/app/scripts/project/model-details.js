// The entry for a model in the project view.
'use strict';
import '../app';
import '../util/util';

angular.module('biggraph').directive('modelDetails', function(util) {
  return {
    restrict: 'E',
    scope: { scalarId: '=' },
    templateUrl: 'scripts/project/model-details.template',
    link: function(scope) {
      scope.showSQL = false;

      scope.model = util.get('/ajax/model', {
        scalarId: scope.scalarId,
      });

      scope.reportError = function() {
        util.reportRequestError(scope.model, 'Error loading model.');
      };
    },
  };
});
