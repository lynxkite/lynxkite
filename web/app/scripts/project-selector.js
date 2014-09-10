'use strict';

angular.module('biggraph').directive('projectSelector', function(util, hotkeys) {
  return {
    restrict: 'E',
    scope: { name: '=', version: '=?' },
    templateUrl: 'project-selector.html',
    link: function(scope, element) {
      hotkeys.bindTo(scope)
        .add({
          combo: 'c', description: 'Create new project',
          callback: function(e) { e.preventDefault(); scope.expandNewProject = true; },
        });
      scope.$watch('expandNewProject', function(ex) {
        if (ex) { element.find('#new-project-name')[0].focus(); }
      });
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
        util.post('/ajax/createProject',
          {
            name: name,
            notes: notes || ''
          }, function() {
            scope.name = name;
          }).then(function() {
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
          util.post('/ajax/discardProject', { name: p }, function() {
            // refresh splash manually
            scope.data = util.nocache('/ajax/splash');
          });
        }
      };
    },
  };
});
