// The list of entries.
'use strict';

angular.module('biggraph').directive('entrySelector',
  function(util, hotkeys, $timeout, $anchorScroll) {
    return {
      restrict: 'E',
      scope: {
        name: '=', // Exposes the name of the selected entry.
        path: '=?', // Starting path.
      },
      templateUrl: 'scripts/splash/entry-selector.html',
      link: function(scope, element) {
        scope.util = util;
        function defaultSettings() {
          return { privacy: 'public-read' };
        }
        scope.opened = {};
        scope.newWorkspace = {};
        scope.newDirectory = defaultSettings();
        scope.path = scope.path || window.sessionStorage.getItem('last_selector_path') ||
          window.localStorage.getItem('last_selector_path') || '';
        const hk = hotkeys.bindTo(scope);
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
          scope.opened = {}; // Snapshot viewers are closed by default.
          if (!scope.searchQuery) {
            scope.data = util.nocache('/ajax/entryList', { path: scope.path });
          } else {
            scope.data = util.nocache(
              '/ajax/entrySearch',
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
          for (let i = 0; i < scope.data.objects.length; ++i) {
            const p = scope.data.objects[i];
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
            for (let i = 0; i < scope.data.objects.length; ++i) {
              const p = scope.data.objects[i];
              scope.vertexCounts[p.name].$abandon();
              scope.edgeCounts[p.name].$abandon();
            }
          }
        }
        scope.$on('$destroy', abandonScalars);

        scope.$on('new table or view', scope.reload);

        scope.createWorkspace = function() {
          scope.newWorkspace.sending = true;
          let name = scope.newWorkspace.name;
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
          let name = scope.newDirectory.name;
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
          const lastSlash = p.lastIndexOf('/');
          return p.slice(lastSlash + 1);
        };
        scope.dirName = function(p) {
          const lastSlash = p.lastIndexOf('/');
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
          if (scope.isSnapshot(o)) { scope.snapshotClick(event, o); }
          return;
        };

        scope.tableClick = function(event, t) {
          // The rename/discard/etc menu is inside the clickable div. Ignore clicks on the menu.
          if (event.originalEvent.alreadyHandled) { return; }
          // Ignore clicks on errored tables.
          if (t.error) { return; }
          const tableNameParts = t.name.split('/');
          const tableName = tableNameParts[tableNameParts.length - 1];
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
          // Ignore clicks on errored entries.
          if (p.error) { return; }
          scope.name = p.name;
        };

        scope.snapshotClick = function(event, p) {
          // The rename/discard/etc menu is inside the clickable div. Ignore clicks on the menu.
          if (event.originalEvent.alreadyHandled) { return; }
          // Ignore clicks on errored entries.
          if (p.error) { return; }
          scope.opened[p.name] = !scope.opened[p.name];
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

        scope.setDirectory = function(index) {
          scope.path = scope.path.split('/').slice(0, index + 1).join('/');
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
            let trashDir = 'Trash';
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
        scope.isSnapshot = function(object) {
          return object.objectType === 'snapshot';
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
