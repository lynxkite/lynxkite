// The entry for a model in the project view.
import '../app';
import '../util/util';
import templateUrl from './model-details.html?url';

angular.module('biggraph').directive('modelDetails', ['util', function(util) {
  return {
    restrict: 'E',
    scope: { scalarId: '=' },
    templateUrl,
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
}]);
