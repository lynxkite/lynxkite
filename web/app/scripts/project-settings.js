'use strict';

angular.module('biggraph').directive('projectSettings', function(util) {
  return {
    restrict: 'E',
    scope: { side: '=', visible: '=' },
    replace: false,
    templateUrl: 'project-settings.html',
    link: function(scope) {
      scope.$watch('side.project.readACL', function(value) {
        scope.readACL = value;
      });
      scope.$watch('side.project.writeACL', function(value) {
        scope.writeACL = value;
      });

      scope.save = function() {
        scope.saving = true;
        util.post('/ajax/changeProjectSettings',
        {
          project: scope.side.project.name,
          readACL: scope.readACL,
          writeACL: scope.writeACL,
        },
        function() {
          scope.visible = false;
          scope.side.reload();
        });
      };

      scope.close = function() {
        scope.visible = false;
      };

      scope.changed = function() {
        return (
            scope.side.project.readACL !== scope.readACL ||
            scope.side.project.writeACL !== scope.writeACL);
      };
    },
  };
});
