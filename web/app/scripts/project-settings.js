// Project-level settings, including ACLs.
'use strict';

angular.module('biggraph').directive('projectSettings', function(util) {
  return {
    restrict: 'E',
    scope: {
      view: '=',
      path: '=',
      acl: '=',
    },
    replace: false,
    templateUrl: 'project-settings.html',
    link: function(scope) {
      scope.$watch('acl.readACL', function(value) {
        scope.readACL = value;
      });
      scope.$watch('acl.writeACL', function(value) {
        scope.writeACL = value;
      });

      scope.save = function() {
        scope.saving = true;
        util.post('/ajax/changeProjectSettings',
        {
          project: scope.path,
          readACL: scope.readACL,
          writeACL: scope.writeACL,
        }).then(function() {
          console.log(scope.view);
          scope.view();
        });
      };

      scope.changed = function() {
        return (
            scope.acl.readACL !== scope.readACL ||
            scope.acl.writeACL !== scope.writeACL);
      };
    },
  };
});
