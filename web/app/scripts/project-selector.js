'use strict';

angular.module('biggraph').directive('projectSelector', function($resource, util) {
  return {
    restrict: 'E',
    scope: { name: '=', version: '=?' },
    templateUrl: 'project-selector.html',
    link: function(scope) {
      scope.util = util;
      scope.data = util.nocache('/ajax/splash');
      scope.$watch('data.version', function(v) { scope.version = v; });
      function scalar(p, name) {
        for (var i = 0; i < p.scalars.length; ++i) {
          if (p.scalars[i].title === name) {
            return p.scalars[i].id;
          }
        }
        return undefined;
      }
      scope.$watch('data.$resolved', function(resolved) {
        if (!resolved) { return; }
        scope.vertexCounts = {};
        scope.edgeCounts = {};
        for (var i = 0; i < scope.data.projects.length; ++i) {
          var p = scope.data.projects[i];
          scope.vertexCounts[p.name] = util.get('/ajax/scalarValue', { scalarId: scalar(p, 'vertex_count') });
          scope.edgeCounts[p.name] = util.get('/ajax/scalarValue', { scalarId: scalar(p, 'edge_count') });
        }
      });
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
      scope.discardProject = function(p) {
        $resource('/ajax/discardProject').save({ name: p }, function() {
          scope.data = util.nocache('/ajax/splash');
        }, function(error) {
          console.error(error);
        });
      };
    },
  };
});
