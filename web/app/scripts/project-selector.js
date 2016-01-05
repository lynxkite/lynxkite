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
      scope.path = (scope.path || window.localStorage.getItem('last_selector_path')) || '';
      hotkeys.bindTo(scope)
        .add({
          combo: 'c', description: 'Create new project',
          callback: function(e) { e.preventDefault(); scope.newProject = { expanded: true }; },
        });

      scope.$watch('newProject.expanded', function(ex) {
        if (ex) {
          $timeout(
            function() {
              element.find('#new-project-name')[0].focus();
            },
            0,
            false); // Do not invoke apply as we don't change the scope.
        }
      });

      scope.$watch('newDirectory.expanded', function(ex) {
        if (ex) {
          $timeout(
            function() {
              element.find('#new-directory-name')[0].focus();
            },
            0,
            false); // Do not invoke apply as we don't change the scope.
        }
      });

      scope.util = util;
      function refresh() {
        abandonScalars();
        if (!scope.searchQuery) {
          scope.data = util.nocache('/ajax/projectList', { path: scope.path });
        } else {
          scope.data = util.nocache(
            '/ajax/projectSearch',
            {
              basePath: scope.path,
              query: scope.searchQuery,
            });
        }
      }

      scope.$watch('path', refresh);
      scope.$watch('searchQuery', refresh);
      function getScalar(title, scalar) {
        var res = util.get('/ajax/scalarValue', {
          scalarId: scalar.id, calculate: false
        });
        res.details = { project: title, scalar: scalar };
        return res;
      }

      // Fake scalar for projects with no vertices/edges.
      var NO = { string: 'no', $abandon: function() {} };

      scope.$watch('data.$resolved', function(resolved) {
        if (!resolved || scope.data.$error) { return; }
        scope.vertexCounts = {};
        scope.edgeCounts = {};
        for (var i = 0; i < scope.data.projects.length; ++i) {
          var p = scope.data.projects[i];
          scope.vertexCounts[p.name] =
            p.vertexCount ? getScalar(p.title, p.vertexCount) : NO;
          scope.edgeCounts[p.name] =
            p.edgeCount ? getScalar(p.title, p.edgeCount) : NO;
        }
      });

      function abandonScalars() {
        if (scope.data && scope.data.$resolved) {
          for (var i = 0; i < scope.data.projects.length; ++i) {
            var p = scope.data.projects[i];
            scope.vertexCounts[p.name].$abandon();
            scope.edgeCounts[p.name].$abandon();
          }
        }
      }
      scope.$on('$destroy', abandonScalars);

      scope.saveLastSelectorPath = function() {
        window.localStorage.setItem('last_selector_path', scope.path);
      };
      scope.createProject = function() {
        scope.newProject.sending = true;
        var name = scope.newProject.name;
        if (scope.path) {
          name = scope.path + '/' + name;
        }
        var notes = scope.newProject.notes;
        util.post('/ajax/createProject',
          {
            name: name,
            notes: notes || '',
            privacy: scope.newProject.privacy,
          }).then(function() {
            scope.name = name;
          }).finally(function() {
            scope.newProject.sending = false;
          });
      };

      scope.createDirectory = function() {
        scope.newDirectory.sending = true;
        var name = scope.newDirectory.name;
        if (scope.path) {
          name = scope.path + '/' + name;
        }
        util.post('/ajax/createDirectory',
          {
            name: name,
          }).then(function() {
            scope.path = name;
            scope.searchQuery = '';
            scope.newDirectory = {};
            scope.saveLastSelectorPath();
          }).finally(function() {
            scope.newDirectory.sending = false;
          });
      };

      scope.baseName = function(p) {
        var lastSlash = p.lastIndexOf('/');
        return p.slice(lastSlash + 1);
      };
      scope.dirName = function(p) {
        var lastSlash = p.lastIndexOf('/');
        return p.slice(0, lastSlash + 1);
      };
      scope.pathInside = function(p) {
        if (scope.path) {
          return p.slice(scope.path.length + 1);
        } else {
          return p;
        }
      };

      scope.projectClick = function(event, p) {
        // The rename/discard/etc menu is inside the clickable div. Ignore clicks on the menu.
        if (event.originalEvent.alreadyHandled) { return; }
        // Ignore clicks on errored projects.
        if (p.error) { return; }
        scope.name = p.name;
      };

      scope.enterDirectory = function(event, d) {
        // The rename/discard/etc menu is inside the clickable div. Ignore clicks on the menu.
        if (event.originalEvent.alreadyHandled) { return; }
        scope.path = d;
        scope.searchQuery = '';
        scope.saveLastSelectorPath();
      };

      scope.popDirectory = function() {
        scope.path = scope.path.split('/').slice(0, -1).join('/');
        scope.searchQuery = '';
        scope.saveLastSelectorPath();
      };

      scope.pathElements = function() {
        return scope.path.split('/');
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
          util.post('/ajax/renameDirectory',
              { from: oldName, to: newName }).then(refresh);
        },
        duplicate: function(kind, p) {
          util.post('/ajax/forkDirectory',
              { from: p, to: scope.dirName(p) + 'Copy of ' + scope.baseName(p) }).then(refresh);
        },
        discard: function(kind, p) {
          var message = 'Permanently delete ' + kind + ' ' + p + '?';
          message += ' (If it is a shared ' + kind + ', it will be deleted for everyone.)';
          if (window.confirm(message)) {
            util.post('/ajax/discardDirectory', { name: p }).then(refresh);
          }
        },
      };
    },
  };
});
