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
      templateUrl: 'scripts/splash/project-selector.html',
      link: function(scope, element) {
        scope.util = util;
        function defaultSettings() {
          return { privacy: 'private' };
        }
        scope.newWorkspace = {};
        scope.newDirectory = defaultSettings();
        scope.path = scope.path || window.sessionStorage.getItem('last_selector_path') ||
          window.localStorage.getItem('last_selector_path') || '';
        var hk = hotkeys.bindTo(scope);
        hk.add({
          combo: 'c', description: 'Create new workspace',
          callback: function(e) { e.preventDefault(); scope.newWorkspace = { expanded: true }; },
        });
        hk.add({
          combo: 'd', description: 'Create new folder',
          callback: function(e) { e.preventDefault(); scope.newDirectory = { expanded: true }; },
        });
        hk.add({
          combo: '/', description: 'Search',
          callback: function(e) { e.preventDefault(); element.find('#search-box').focus(); },
        });

        scope.$watch('newWorkspace.expanded', function(ex) {
          if (ex) {
            $timeout(
              function() {
                element.find('#new-workspace-name')[0].focus();
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

        scope.createWorkspace = function() {
          scope.newWorkspace.sending = true;
          var name = scope.newWorkspace.name;
          if (scope.path) {
            name = scope.path + '/' + name;
          }
          util.post('/ajax/createWorkspace',
            {
              name: name,
            }).then(function() {
              scope.name = name;
            }).finally(function() {
              scope.newWorkspace.sending = false;
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
          if (scope.isWorkspace(o)) { scope.workspaceClick(event, o); }
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

        scope.workspaceClick = function(event, p) {
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

        scope.menu = {
          rename: function(kind, oldName, newName) {
            if (oldName === newName) { return; }
            util.post('/ajax/renameEntry',
                { from: oldName, to: newName, overwrite: false }).then(scope.reload);
          },
          duplicate: function(kind, p) {
            util.post('/ajax/forkEntry', {
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
              scope.showSQL = true;
              $anchorScroll('global-sql-box');
              $timeout(function () {
                scope.$broadcast('fill sql-box from config and clear sql result', name, config, type);
              });
              return;
            } else {
              scope.startTableImport();
              $timeout(function () {
                $anchorScroll('import-table');
                var fullName = name.split('/');
                var relativeName = fullName[fullName.length - 1];
                scope.$broadcast('fill import from config', config, relativeName, type);
              });
            }
          },
          renameMenuItemLabel: 'Rename or move...'
        };

        scope.isWorkspace = function(object) {
          return object.objectType === 'workspace';
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
