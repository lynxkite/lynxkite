// UI for importing external data.
'use strict';

angular.module('biggraph').directive('importWizard', function(util) {
  return {
    scope: { expanded: '=' },
    templateUrl: 'import-wizard.html',
    link: function(scope) {
      scope.csv = {
        delimiter: ',',
        mode: 'FAILFAST',
      };
      scope.cancel = function(event) {
        scope.expanded = false;
        event.stopPropagation();
      };
      scope.reportError = function() {
        util.reportRequestError(scope.importResult, 'Error importing table.');
      };
      scope.importCSV = function() {
        scope.inputsDisabled = true;
        scope.importInProgress = true;
        scope.importResult = util.post(
          '/ajax/importCSV',
          {
            files: scope.csv.filename,
            columnNames: scope.csv.columnNames ? [] : [],
            delimiter: scope.csv.delimiter,
            mode: scope.csv.mode,
          });
        scope.importResult.catch(function() {
          scope.inputsDisabled = false;
        });
        scope.importResult.finally(function() {
          scope.importInProgress = false;
        });
      };
      scope.saveTable = function() {
        util.post(
          '/ajax/saveTable',
          {
            'tableName': scope.tableName,
            'checkpoint': scope.importResult.checkpoint,
            'privacy': 'public-read',
          });
      };
    },
  };
});
