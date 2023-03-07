// Project-level settings, including ACLs.
import '../app';
import '../util/util';
import templateUrl from './acl-settings.html?url';

angular.module('biggraph').directive('aclSettings', ['util', function(util) {
  return {
    restrict: 'E',
    scope: {
      reload: '&',
      path: '=',
      entity: '=',
    },
    replace: false,
    templateUrl,
    link: function(scope) {
      scope.$watch('entity.readACL', function(value) {
        scope.readACL = value;
      });
      scope.$watch('entity.writeACL', function(value) {
        scope.writeACL = value;
      });

      scope.save = function() {
        scope.saving = true;
        util.post('/ajax/changeACLSettings', {
          project: scope.path,
          readACL: scope.readACL,
          writeACL: scope.writeACL,
        }).then(function() {
          scope.reload();
        });
      };

      scope.changed = function() {
        return (
          scope.entity.readACL !== scope.readACL ||
            scope.entity.writeACL !== scope.writeACL);
      };
    },
  };
}]);
