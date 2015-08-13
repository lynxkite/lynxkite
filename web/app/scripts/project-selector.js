// The list of projects.
'use strict';

angular.module('biggraph').directive('projectSelector', function(util, hotkeys, $timeout) {
  return {
    restrict: 'E',
    scope: {
      name: '=', // Exposes the name of the selected project.
      path: '=?', // Starting path.
    },
    templateUrl: 'project-selector.html',
    link: function(scope, element) {
      scope.path = scope.path || '';
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
        scope.data = util.nocache('/ajax/projectList', { path: scope.path });
      }
      scope.$watch('path', refresh);
      function getScalar(title, scalar) {
        var res = util.get('/ajax/scalarValue', {
          scalarId: scalar.id, calculate: false
        });
        res.details = { project: title, scalar: scalar };
        return res;
      }
      scope.$watch('data.$resolved', function(resolved) {
        if (!resolved || scope.data.$error) { return; }
        scope.vertexCounts = {};
        scope.edgeCounts = {};
        for (var i = 0; i < scope.data.projects.length; ++i) {
          var p = scope.data.projects[i];
          scope.vertexCounts[p.name] =
            p.vertexCount ? getScalar(p.title, p.vertexCount) : { string: 'no' };
          scope.edgeCounts[p.name] =
            p.edgeCount ? getScalar(p.title, p.edgeCount) : { string: 'no' };
        }
      });
      scope.createProject = function() {
        scope.newProject.sending = true;
        var name = scope.newProject.name.replace(/ /g, '_');
        if (scope.path) {
          name = scope.path + '/' + name;
        }
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

      scope.enterDirectory = function(d) {
        if (scope.path) {
          scope.path += '/' + d;
        } else {
          scope.path = d;
        }
      };

      scope.popDirectory = function() {
        scope.path = scope.path.split('/').slice(0, -1).join('/');
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
