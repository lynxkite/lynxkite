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
      function getScalar(p, name) {
        for (var i = 0; i < p.scalars.length; ++i) {
          if (p.scalars[i].title === name) {
            return util.get('/ajax/scalarValue', { scalarId: p.scalars[i].id });
          }
        }
        return { value: undefined };
      }
      scope.$watch('data.$resolved', function(resolved) {
        if (!resolved) { return; }
        scope.vertexCounts = {};
        scope.edgeCounts = {};
        for (var i = 0; i < scope.data.projects.length; ++i) {
          var p = scope.data.projects[i];
          scope.vertexCounts[p.name] = p.vertexSet ? getScalar(p, 'vertex_count') : { value: 'no' };
          scope.edgeCounts[p.name] = p.edgeBundle ? getScalar(p, 'edge_count') : { value: 'no' };
        }
      });
      scope.createProject = function() {
        scope.newProject.sending = true;
        var name = scope.newProject.name.replace(/ /g, '_');
        var notes = scope.newProject.notes;
        $resource('/ajax/createProject').save({ name: name, notes: notes || '' }, function() {
          scope.name = name;
        }, function(error) {
          util.ajaxError(error);
          scope.newProject.sending = false;
        });
      };
      scope.setProject = function(p) {
        scope.name = p;
      };
      scope.discardProject = function(p, event) {
        // avoid opening project or refreshing splash automatically
        event.preventDefault();
        event.stopPropagation();
        if (window.confirm('Are you sure you want to discard project ' + util.spaced(p) + '?')) {
          $resource('/ajax/discardProject').save({ name: p }, function() {
            // refresh splash manually
            scope.data = util.nocache('/ajax/splash');
          }, function(error) {
            util.ajaxError(error);
          });
        }
      };
    },
  };
});
