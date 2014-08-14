'use strict';

angular.module('biggraph').directive('projectSelector', function($resource, util) {
  return {
    restrict: 'E',
    scope: { name: '=' },
    templateUrl: 'project-selector.html',
    link: function(scope) {
      scope.util = util;
      scope.data = util.nocache('/ajax/splash');
      scope.createProject = function() {
        scope.newProject.sending = true;
        var name = scope.newProject.name.replace(/ /g, '_');
        var notes = scope.newProject.notes;
        $resource('/ajax/createProject').save({ name: name, notes: notes || '' }, function() {
          scope.name = name;
        }, function(error) {
          console.error(error);
          scope.newProject.sending = false;
        });
      };
      scope.setProject = function(p) {
        scope.name = p;
      };
    },
  };
});
