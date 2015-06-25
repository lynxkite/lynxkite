// The list of projects.
'use strict';

angular.module('biggraph').directive('projectSelector', function(util, hotkeys, $timeout) {
  return {
    restrict: 'E',
    scope: {
      name: '=', // Exposes the name of the selected project.
      version: '=?',  // Exposes the version string.
    },
    templateUrl: 'project-selector.html',
    link: function(scope, element) {
      hotkeys.bindTo(scope)
        .add({
          combo: 'c', description: 'Create new project',
          callback: function(e) { e.preventDefault(); scope.expandNewProject = true; },
        });
      scope.$watch('expandNewProject', function(ex) {
        if (ex) {
          $timeout(
            function() {
              element.find('#new-project-name')[0].focus();
            },
            0,
            false); // Do not invoke apply as we don't change the scope.
        }
      });
      scope.util = util;
      function refresh() {
        scope.data = util.nocache('/ajax/splash');
      }
      refresh();
      scope.$watch('data.version', function(v) { scope.version = v; });
      function getScalar(p, name) {
        for (var i = 0; i < p.scalars.length; ++i) {
          if (p.scalars[i].title === name) {
            var res = util.get('/ajax/scalarValue', {
              scalarId: p.scalars[i].id, calculate: false
            });
            res.details = { project: p.title, scalar: p.scalars[i] };
            return res;
          }
        }
        return { error: 'Attribute not found: ' + name };
      }
      scope.$watch('data.$resolved', function(resolved) {
        if (!resolved || scope.data.$error) { return; }
        scope.vertexCounts = {};
        scope.edgeCounts = {};
        for (var i = 0; i < scope.data.projects.length; ++i) {
          var p = scope.data.projects[i];
          scope.vertexCounts[p.name] =
            p.hasVertices ? getScalar(p, 'vertex_count') : { string: 'no' };
          scope.edgeCounts[p.name] =
            p.hasEdges ? getScalar(p, 'edge_count') : { string: 'no' };
        }
      });
      scope.createProject = function() {
        scope.newProject.sending = true;
        var name = scope.newProject.name.replace(/ /g, '_');
        var notes = scope.newProject.notes;
        util.post('/ajax/createProject',
          {
            name: name,
            notes: notes || '',
            privacy: scope.newProject.privacy,
          }, function() {
            scope.name = name;
          }).$status.then(function() {
            scope.newProject.sending = false;
          });
      };

      scope.projectClick = function(event, p) {
        // The rename/discard/etc menu is inside the clickable div. Ignore clicks on the menu.
        if (event.originalEvent.alreadyHandled) { return; }
        // Ignore clicks on errored projects.
        if (p.error) { return; }
        scope.name = p.name;
      };

      scope.reportListError = function() {
        util.reportRequestError(scope.data, 'Project list could not be loaded.');
      };

      scope.reportProjectError = function(project) {
        util.reportError({ message: project.error, details: scope.data });
      };

      scope.menu = {
        rename: function(kind, oldName, newName) {
          if (oldName === newName) { return; }
          util.post('/ajax/renameProject', { from: oldName, to: newName }, refresh);
        },
        duplicate: function(kind, p) {
          util.post('/ajax/forkProject', { from: p, to: 'Copy of ' + p }, refresh);
        },
        discard: function(kind, p) {
          var message = 'Permanently delete project ' + util.spaced(p) + '?';
          message += ' (If it is a shared project, it will be deleted for everyone.)';
          if (window.confirm(message)) {
            util.post('/ajax/discardProject', { name: p }, refresh);
          }
        }
      };
    },
  };
});
