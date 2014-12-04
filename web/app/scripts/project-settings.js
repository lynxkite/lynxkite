'use strict';

angular.module('biggraph').directive('projectSettings', function(util) {
  return {
    restrict: 'E',
    scope: { project: '=', visible: '=' },
    replace: false,
    templateUrl: 'project-settings.html',
    link: function(scope) {
      scope.$watch('project.readACL', function(value) {
        scope.readACL = value;
      });
      scope.$watch('project.writeACL', function(value) {
        scope.writeACL = value;
      });

      scope.save = function() {
        scope.saving = true;
        util.post('/ajax/changeProjectSettings',
        {
          project: scope.project.name,
          readACL: scope.readACL,
          writeACL: scope.writeACL,
        },
        function() {
          scope.visible = false;
        });
      };

      scope.close = function() {
        scope.visible = false;
      };

      scope.changed = function() {
        return scope.project.readACL !== scope.readACL || scope.project.writeACL !== scope.writeACL;
      };
    },
  };
});
