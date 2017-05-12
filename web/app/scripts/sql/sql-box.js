// Presents the parameters for running SQL scripts.
'use strict';

angular.module('biggraph').directive('sqlBox', function($rootScope, $window, $q, side, util) {
  return {
    restrict: 'E',
    scope: {
      side: '=?',
      directory: '=?',
    },
    templateUrl: 'scripts/sql/sql-box.html',
    link: function(scope) {
      scope.inProgress = 0;
      scope.directoryDefined = (typeof scope.directory !== 'undefined');
      scope.maxRows = 10;
      scope.maxPersistedHistoryLength = 100;

      if (!!scope.side && scope.directoryDefined) {
        throw 'can not be both defined: scope.side, scope.directory';
      }
      if (!scope.side && !scope.directoryDefined) {
        throw 'one of them needs to be defined: scope.side, scope.directory';
      }
      scope.isGlobal = !scope.side;
      scope.sql = scope.isGlobal ? 'select * from `directory/project|vertices`' :
       'select * from vertices';

      function SqlHistory(maxLength) {
        // This is a helper class for storing sql query history in localStorage.
        // The localStorage contains a limited number of the most recent queries, and all
        // sql boxes synchronize with it on creation. Every sql box maintains an array of its
        // own local history, but also pushes newly executed queries into localStorage.
        // Although the query currently being edited is not yet part of the history, it's
        // stored as this.history[0] for syntactic convenience.

        // Load persisted sql history
        this.loadGlobalHistory = function() {
          var history;
          try {
            history = angular.fromJson(window.localStorage.getItem('sqlHistory'));
            if (!Array.isArray(history)) {
              throw 'sqlHistory is not an array';
            }
          } catch (e) {
            history = [];
            window.localStorage.setItem('sqlHistory', angular.toJson([]));
          }
          return history;
        };

        // Initialize
        this.maxLength = maxLength;
        this.history = this.loadGlobalHistory();
        // Store current query as first element
        this.history.unshift(scope.sql);
        this.index = 0;

        // Save current query
        this.saveCurrentQuery = function() {
          this.index = 0;
          this.history[0] = scope.sql;
          // Insert current query into our local subset of history
          this.history.unshift(this.history[0]);
          // Insert current query into a copy of global history
          var history = this.loadGlobalHistory();
          history.unshift(this.history[0]);
          while (history.length > maxLength) {
            history.pop();
          }
          // Update global history
          window.localStorage.setItem('sqlHistory', angular.toJson(history));
        };
        // Move one row up in local history
        this.navigateUp = function() {
          if (this.index < this.history.length - 1) {
            if (this.index === 0) {
              // Update current query in local history
              this.history[0] = scope.sql;
            }
            this.index++;
            scope.sql = this.history[this.index];
          }
        };
        // Move one row down in local history
        this.navigateDown = function() {
          if (this.index > 0) {
            this.index--;
            scope.sql = this.history[this.index];
          }
        };
      }
      scope.sqlHistory = new SqlHistory(scope.maxPersistedHistoryLength);

      scope.project = scope.side && scope.side.state.projectName;
      scope.overwrite = false;
      scope.sort = {
        column: undefined,
        reverse: false,
        select: function(index) {
          index = index.toString();
          if (scope.sort.column === index) {
            if (scope.sort.reverse) {
              // Already reversed by this column. This click turns off sorting.
              scope.sort.column = undefined;
            } else {
              // Already sorting by this column. This click reverses.
              scope.sort.reverse = true;
            }
          } else {
            // Not sorted yet. This click sorts by this column.
            scope.sort.column = index;
            scope.sort.reverse = false;
          }
        },
        style: function(index) {
          index = index.toString();
          if (index === scope.sort.column) {
            return scope.sort.reverse ? 'sort-desc' : 'sort-asc';
          }
        },
      };

      scope.sortKey = function(a) {
        var col = scope.sort.column;
        var dv = a[col];
        return dv && dv.defined && (dv.double !== undefined ? dv.double : dv.string);
      };

      scope.runSQLQuery = function() {
        if (!scope.sql) {
          scope.result = { $error: 'SQL script must be specified.' };
        } else {
          scope.sqlHistory.saveCurrentQuery();
          scope.inProgress += 1;
          scope.result = util.nocache(
            '/ajax/runSQLQuery',
            {
              dfSpec: {
                directory: scope.directory,
                project: scope.project,
                sql: scope.sql,
              },
              maxRows: parseInt(scope.maxRows),
            });
          scope.result.finally(function() {
            scope.inProgress -= 1;
          });
        }
      };

      scope.offerWithPath = function() {
        if (typeof scope.directory !== 'undefined') {
          if (scope.directory === '') {
            return '';
          } else {
            return scope.directory + '/';
          }
        } else {
          if (typeof scope.project !== 'undefined') {
            var proj = scope.project;
            var lastSepIndex = proj.lastIndexOf('/');
            return proj.substring(0, lastSepIndex + 1);
          } else {
            return '';
          }
        }
      };

      scope.$watch('exportFormat', function(exportFormat) {
        if (exportFormat === 'table' ||
            exportFormat === 'view') {
          scope.exportKiteTable = scope.exportKiteTable || scope.offerWithPath();
        } else if (exportFormat === 'segmentation') {
          scope.exportSegmentation = scope.exportSegmentation || '';
        } else if (exportFormat === 'csv') {
          scope.exportPath = '<download>';
          scope.exportDelimiter = ',';
          scope.exportQuote = '"';
          scope.exportHeader = true;
        } else if (exportFormat === 'json') {
          scope.exportPath = '<download>';
        } else if (exportFormat === 'parquet') {
          scope.exportPath = '';
        } else if (exportFormat === 'orc') {
          scope.exportPath = '';
        } else if (exportFormat === 'jdbc') {
          scope.exportJdbcUrl = '';
          scope.exportJdbcTable = '';
          scope.exportMode = 'error';
        }
      });

      scope.export = function(options) {
        options = options || { overwrite: false };
        var req = {
          dfSpec: {
            isGlobal: scope.isGlobal,
            directory: scope.directory,
            project: scope.project,
            sql: scope.sql,
          },
          overwrite: options.overwrite || scope.overwrite,
        };

        scope.inProgress += 1;
        var result;
        var postOpts = { reportErrors: false };
        if (scope.exportFormat === 'table') {
          req.table = scope.exportKiteTable;
          req.privacy = 'public-read';
          result = util.post('/ajax/exportSQLQueryToTable', req, postOpts);
        } else if (scope.exportFormat === 'segmentation') {
          result = scope.side.applyOp(
            'Create-segmentation-from-SQL', {
              name: scope.exportSegmentation,
              sql: scope.sql
            });
        } else if (scope.exportFormat === 'view') {
          req.name = scope.exportKiteTable;
          req.privacy = 'public-read';
          result = util.post('/ajax/createViewDFSpec', req, postOpts);
        } else if (scope.exportFormat === 'csv') {
          req.path = scope.exportPath;
          req.delimiter = scope.exportDelimiter;
          req.quote = scope.exportQuote;
          req.header = scope.exportHeader;
          result = util.post('/ajax/exportSQLQueryToCSV', req, postOpts);
        } else if (scope.exportFormat === 'json') {
          req.path = scope.exportPath;
          result = util.post('/ajax/exportSQLQueryToJson', req, postOpts);
        } else if (scope.exportFormat === 'parquet') {
          req.path = scope.exportPath;
          result = util.post('/ajax/exportSQLQueryToParquet', req, postOpts);
        } else if (scope.exportFormat === 'orc') {
          req.path = scope.exportPath;
          result = util.post('/ajax/exportSQLQueryToORC', req, postOpts);
        } else if (scope.exportFormat === 'jdbc') {
          req.jdbcUrl = scope.exportJdbcUrl;
          req.table = scope.exportJdbcTable;
          req.mode = scope.exportMode;
          result = util.post('/ajax/exportSQLQueryToJdbc', req, postOpts);
        } else {
          throw new Error('Unexpected export format: ' + scope.exportFormat);
        }

        result.catch(function exportErrorHandler(error) {
          if (error.data === 'file-already-exists-confirm-overwrite') {
            util.showOverwriteDialog(function() {
              scope.export({ overwrite: true });
            });
          } else {
            util.ajaxError(error);
          }
          return $q.reject(error);
        }).then(function exportDoneHandler(result) {
          scope.showExportOptions = false;
          scope.success = 'Results exported.';
          if (result && result.download) {
            // Fire off the download.
            $window.location =
              '/downloadFile?q=' + encodeURIComponent(JSON.stringify(result.download));
          }
          if (scope.exportFormat === 'table' || scope.exportFormat === 'view') {
            $rootScope.$broadcast('new table or view', scope);
          }
        }).finally(function() {
          scope.inProgress -= 1;
        });
      };

      scope.$watch('showExportOptions', function(showExportOptions) {
        if (showExportOptions) {
          scope.success = ''; // Hide previous success message to avoid confusion.
        }
      });

      scope.reportSQLError = function() {
        util.reportRequestError(scope.result, 'Error executing query.');
      };

      scope.onLoad = function(editor) {
        editor.setOptions({
          autoScrollEditorIntoView : true,
          maxLines : 500
        });

        editor.commands.addCommand({
          name: 'navigateUp',
          bindKey: {
            win: 'Ctrl-Up',
            mac: 'Command-Up',
            sender: 'editor|cli'
          },
          exec: function() { scope.$apply(function() { scope.sqlHistory.navigateUp(); }); }
        });
        editor.commands.addCommand({
          name: 'navigateDown',
          bindKey: {
            win: 'Ctrl-Down',
            mac: 'Command-Down',
            sender: 'editor|cli'
          },
          exec: function() { scope.$apply(function() { scope.sqlHistory.navigateDown(); }); }
        });
        editor.commands.addCommand({
          name: 'submit',
          bindKey: {
            win: 'Ctrl-Enter',
            mac: 'Command-Enter',
            sender: 'editor|cli'
          },
          exec: function() { scope.$apply(function() { scope.runSQLQuery(); }); },
        });
      };

      scope.$on('fill sql-box from config and clear sql result', function(evt, name, config, type) {
        scope.sql = config.data.dfSpec.sql;
        scope.directory = config.data.dfSpec.directory;
        scope.project = config.data.dfSpec.project;
        scope.exportFormat = type;
        scope.exportKiteTable = name;
        scope.overwrite = true;
        scope.result = '';
      });

      scope.$on('fill sql-box by clicking on table or view', function(event, tableName) {
        scope.sql = 'select * from `' + tableName + '`';
        scope.runSQLQuery();
      });

      scope.showMoreRowsIncrement = function() {
        // Offer increases of 10, 100, 1000, etc. depending on the magnitude of the current limit.
        return Math.max(10, Math.pow(10, Math.floor(Math.log10(scope.maxRows))));
      };

      scope.showMoreRows = function() {
        scope.maxRows += scope.showMoreRowsIncrement();
        scope.runSQLQuery();
      };
    }
  };
});
