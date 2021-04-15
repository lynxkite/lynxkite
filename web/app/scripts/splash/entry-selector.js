// The list of entries.
'use strict';

angular.module('biggraph').directive('entrySelector',
  function(util, hotkeys, $timeout, $anchorScroll, $location, $routeParams) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/splash/entry-selector.html',
      link: function(scope, element) {
        const md = window.markdownit();
        scope.util = util;
        function defaultSettings() {
          return { privacy: 'public-read' };
        }
        scope.opened = {};
        scope.newWorkspace = {};
        scope.newDirectory = defaultSettings();
        scope.path = $routeParams.directoryName ? $routeParams.directoryName.slice(1) : undefined;
        scope.$watch('path', p => {
          if (p === undefined) {
            return;
          }
          // We don't need a reload for directory navigation, but we track the path in the URL.
          const url = '/dir/' + p;
          if (url !== decodeURIComponent($location.url())) {
            util.skipReload();
            $location.url(url);
          }
        });
        let folderDescriptions = {};
        util.frontendConfig.then(cfg => {
          folderDescriptions = cfg.folderDescriptions || {};
          if (scope.path === undefined) {
            // If the path isn't in the URL, we use the defaultFolder setting or the user directory.
            if (cfg.defaultFolder !== undefined) {
              scope.path = cfg.defaultFolder;
            } else {
              util.user.then(user => {
                const email = user.email;
                if (email && email !== '(not logged in)' && email !== '(single-user)') {
                  scope.path = 'Users/' + user.email;
                } else {
                  scope.path = '';
                }
              });
            }
          }
        });
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
          let req;
          if (!scope.searchQuery) {
            req = util.nocache('/ajax/entryList', { path: scope.path });
          } else {
            req = util.nocache(
              '/ajax/entrySearch',
              {
                basePath: scope.path,
                query: scope.searchQuery,
                includeNotes: true,
              });
          }
          scope.nextData = req;
          req.then(res => {
            if (req === scope.nextData) {
              if (util.user.wizardOnly) {
                filterWizardOnly(res);
              }
              scope.data = res;
              delete scope.nextData;
              setFolderDescription();
            }
          });
        };

        // Wizard-only users still have access to custom boxes and such because they
        // need it to execute the workspaces and it's also useful if they want to learn
        // LynxKite. But we hide these on the UI to create a more focused view.
        function filterWizardOnly(data) {
          data.directories = data.directories.filter(d => d !== 'built-ins' && d !== 'custom_boxes');
        }

        let reloadCalled = false;
        function basicWatch(after, before) {
          scope.opened = {};
          if (scope.path === undefined) {
            // Put off reload() calls until path is eventually initialized.
            return;
          }
          const realChange = before !== after;
          if (realChange || !reloadCalled) {
            reloadCalled = true;
            scope.reload();
          }
        }

        function setFolderDescription() {
          let best = 0;
          scope.description = '';
          for (let k of Object.keys(folderDescriptions)) {
            const r = RegExp('^' + k + '$');
            if (k.length >= best && scope.path.match(r)) {
              best = k.length;
              scope.description = md.render(folderDescriptions[k]);
            }
          }
        }

        scope.$watch('path', basicWatch);
        scope.$watch('searchQuery', basicWatch);
        scope.$on('saved snapshot', scope.reload);

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
          util.post('/ajax/createWorkspace', { name: name }).then(function() {
            $location.url('/workspace/' + name);
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

        scope.objectClick = function(event, o) {
          if (scope.isWorkspace(o)) { scope.workspaceClick(event, o); }
          if (scope.isWizard(o)) { scope.wizardClick(event, o); }
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

        scope.wizardClick = function(event, p) {
          if (event.originalEvent.alreadyHandled) { return; }
          if (p.error) { return; }
          $location.url('/wizard/' + p.name);
        };

        scope.workspaceClick = function(event, p) {
          // The rename/discard/etc menu is inside the clickable div. Ignore clicks on the menu.
          if (event.originalEvent.alreadyHandled) { return; }
          // Ignore clicks on errored entries.
          if (p.error) { return; }
          $location.url('/workspace/' + p.name);
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
          return scope.path ? scope.path.split('/') : [];
        };

        scope.isWorkspace = function(object) {
          return object.objectType === 'workspace';
        };
        scope.isWizard = function(object) {
          return object.objectType.includes('wizard');
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

        function showTutorial() {
          if (!util.user.$resolved) { // Wait for user data before deciding to show it.
            util.user.then(showTutorial);
            return;
          }
          if (util.user.wizardOnly
            || localStorage.getItem('entry-selector tutorial done')
            && localStorage.getItem('allow data collection')) {
            return;
          }
          /* global Tour */
          scope.tutorial = new Tour({
            autoscroll: false,
            framework: 'bootstrap3',
            storage: null,
            backdrop: true,
            showProgressBar: false,
            sanitizeFunction: x => x,
            steps: [
              {
                orphan: true,
                animation: false,
                content: () => {
                  if (util.globals.dataCollectionMode === 'optional') {
                    dataCollectionCheckboxChecked =
                      util.collectUsage || !localStorage.getItem('allow data collection');
                    return `
                <p><b>Welcome to LynxKite!</b>
                <p>This seems to be your first visit. I can quickly show you how to get started.
                <p><label><input
                  type="checkbox" id="allow-data-collection" ${dataCollectionCheckboxChecked ? 'checked' : ''}
                  onchange="dataCollectionCheckboxChanged(this);">
                  Share anonymous usage statistics</label>
                <p><a href="https://lynxkite.com/anonymous-usage-statistics">What do we collect?</a>
                    `;
                  } else if (util.globals.dataCollectionMode === 'always') {
                    return `
                <p><b>Welcome to LynxKite!</b>
                <p>This seems to be your first visit. I can quickly show you how to get started.
                <p>The LynxKite team collects anonymous usage statistics when you use this instance.
                (<a href="https://lynxkite.com/anonymous-usage-statistics">What do we collect?</a>)
                    `;
                  } else {
                    return `
                <p><b>Welcome to LynxKite!</b>
                <p>This seems to be your first visit. I can quickly show you how to get started.
                    `;
                  }
                },
              }, {
                placement: 'top',
                element: '.user-menu-dropup',
                animation: false,
                content: () => {
                  if (util.globals.dataCollectionMode === 'optional') {
                    return `
                <p>If you wish to see this tutorial again, you can find it in the
                hamburger menu.
                <p>Use it if you change your mind about providing anonymous usage data.
                    ` + (util.collectUsage ? ' (Thank you for signing up!)' : '');
                  } else {
                    return `
                <p>If you wish to see this tutorial again, you can find it in the
                hamburger menu.
                    `;
                  }
                },
              }, {
                placement: 'top',
                element: '#directory-browser',
                content: `
                <p>LynxKite stores your work in a virtual file system that you can see here.
                You can open, rename, or delete files, organize them in folders, and so on.

                <p>You can have two kinds of files: <b>workspaces</b> and <b>snapshots</b>.
                `,
                animation: false,
              }, {
                placement: 'top',
                element: '#new-workspace',
                content: `
                <p>Click here to create a new <b>workspace</b>.

                <p>A workspace is the place for stringing together steps of a data analysis pipeline.

                <p><i>The tutorial will continue when you've created a workspace.</i>
                `,
                animation: false,
              },
            ],
            onNext: function(tour) {
              if (tour.getCurrentStepIndex() === 0) {
                scope.$apply(() => util.allowDataCollection(dataCollectionCheckboxChecked));
              }
            },
            onEnd: function(tour) {
              localStorage.setItem('entry-selector tutorial done', 'true');
              if (tour.getCurrentStepIndex() === 0) {
                scope.$apply(() => util.allowDataCollection(dataCollectionCheckboxChecked));
              }
            },
            onShown: function() {
              if (scope.tutorial.getCurrentStepIndex() === scope.tutorial.getStepCount() - 1) {
                // Last step reached. We're done.
                localStorage.setItem('entry-selector tutorial done', 'true');
              }
            },
          });
          scope.tutorial.start();
        }
        showTutorial();
        scope.$on('start tutorial', function() {
          localStorage.removeItem('entry-selector tutorial done');
          showTutorial();
        });
        scope.$on('$destroy', function() { scope.tutorial && scope.tutorial.end(); });
      },
    };
  });

// We store this in a global variable because the checkbox is outside of Angular.
let dataCollectionCheckboxChecked;
/* eslint-disable no-unused-vars */ // This is used by the tutorial.
function dataCollectionCheckboxChanged(e) {
  dataCollectionCheckboxChecked = e.checked;
}
