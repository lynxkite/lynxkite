// The list of projects.
'use strict';

angular.module('biggraph').directive('projectSelector',
  function(util, hotkeys, $timeout, $anchorScroll) {
  return {
    restrict: 'E',
    scope: {
      name: '=', // Exposes the name of the selected project.
      path: '=?', // Starting path.
    },
    templateUrl: 'project-selector.html',
    link: function(scope, element) {
      scope.util = util;
      function defaultSettings() {
        return { privacy: 'private' };
      }
      scope.newProject = defaultSettings();
      scope.newDirectory = defaultSettings();
      scope.path = scope.path || window.sessionStorage.getItem('last_selector_path') ||
        window.localStorage.getItem('last_selector_path') || '';
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
      scope.reload = function() {
        abandonScalars();
        if (!scope.searchQuery) {
          scope.data = util.nocache('/ajax/projectList', { path: scope.path });
        } else {
          scope.data = util.nocache(
            '/ajax/projectSearch',
            {
              basePath: scope.path,
              query: scope.searchQuery,
              includeNotes: true,
            });
        }
        window.sessionStorage.setItem('last_selector_path', scope.path);
        window.localStorage.setItem('last_selector_path', scope.path);
      };

      scope.$watch('path', scope.reload);
      scope.$watch('searchQuery', scope.reload);

      scope.$watch('data.$resolved', function(resolved) {
        if (!resolved || scope.data.$error) { return; }
        scope.vertexCounts = {};
        scope.edgeCounts = {};
        for (var i = 0; i < scope.data.objects.length; ++i) {
          var p = scope.data.objects[i];
          scope.vertexCounts[p.name] = util.lazyFetchScalarValue(
            p.vertexCount,
            false);
          scope.edgeCounts[p.name] = util.lazyFetchScalarValue(
            p.edgeCount,
            false);
        }
      });

      function abandonScalars() {
        if (scope.data && scope.data.$resolved && !scope.data.$error) {
          for (var i = 0; i < scope.data.objects.length; ++i) {
            var p = scope.data.objects[i];
            scope.vertexCounts[p.name].$abandon();
            scope.edgeCounts[p.name].$abandon();
          }
        }
      }
      scope.$on('$destroy', abandonScalars);

      scope.$on('new table or view', scope.reload);

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
            privacy: scope.newDirectory.privacy,
          }).then(function() {
            scope.path = name;
            scope.searchQuery = '';
            scope.newDirectory = defaultSettings();
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

      scope.objectClick = function(event, o) {
        if (scope.isProject(o)) { scope.projectClick(event, o); }
        if (scope.isTable(o) || scope.isView(o)) { scope.tableClick(event, o); }
        return;
      };

      scope.tableClick = function(event, t) {
        // The rename/discard/etc menu is inside the clickable div. Ignore clicks on the menu.
        if (event.originalEvent.alreadyHandled) { return; }
        // Ignore clicks on errored tables.
        if (t.error) { return; }
        var tableNameParts = t.name.split('/');
        var tableName = tableNameParts[tableNameParts.length - 1];
        scope.showSQL = true;
        $timeout(
          function() {
            $anchorScroll('global-sql-box');
            scope.$broadcast('fill sql-box by clicking on table or view', tableName);
          },
          0,
          false); // Do not invoke apply as we don't change the scope.
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
      };

      scope.popDirectory = function() {
        scope.path = scope.path.split('/').slice(0, -1).join('/');
        scope.searchQuery = '';
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
          util.post('/ajax/renameEntry',
              { from: oldName, to: newName, overwrite: false }).then(scope.reload);
        },
        duplicate: function(kind, p) {
          util.post('/ajax/forkEntry',
              {
                from: p,
                to: scope.dirName(p) + 'Copy of ' + scope.baseName(p)
              }).then(scope.reload);
        },
        discard: function(kind, p) {
          var trashDir = 'Trash';
          if (util.globals.hasAuth) {
            // Per-user trash.
            trashDir = util.user.home + '/Trash';
          }
          if (p.indexOf(trashDir) === 0) {
            // Already in Trash. Discard permanently.
            util.post('/ajax/discardEntry', { name: p }).then(scope.reload);
          } else {
            // Not in Trash. Move to Trash.
            util.post('/ajax/renameEntry',
                { from: p, to: trashDir + '/' + p, overwrite: true }).then(scope.reload);
          }
        },
        editConfig: function(name, config, type) {
          if (config.class.includes('SQL')) {
            scope.showSQL=true;
            $anchorScroll('global-sql-box');
            $timeout(function () {
              scope.$broadcast('fill sql-box from config and clear sql result', name, config, type);
            });
            return;
          } else {
            scope.startTableImport();
            $timeout(function () {
              $anchorScroll('import-table');
              scope.$broadcast('fill import from config', config, name, type);
            });
          }
        },
        renameMenuItemLabel: 'Rename or move...'
      };

      scope.isProject = function(object) {
        return object.objectType === 'project';
      };
      scope.isTable = function(object) {
        return object.objectType === 'table';
      };
      scope.isView = function(object) {
        return object.objectType === 'view';
      };

      scope.importTable = {};
      scope.startTableImport = function() {
        if (!scope.importTable.expanded) {
          scope.importTable.expanded = true;
          scope.importTable.tableImported = undefined;
        }
      };
      scope.$watch('importTable.tableImported', function(table) {
        if (table !== undefined) {
          scope.importTable.expanded = false;
          scope.reload();
        }
      });
    },
  };
});
