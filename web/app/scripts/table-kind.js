// UI for the table selector operation parameter kind
'use strict';

angular.module('biggraph').directive('tableKind', function() {
  return {
    scope: {
      tables: '=',
      selected: '=model',
      editable: '=',
      fileUploads: '=',
    },
    templateUrl: 'table-kind.html',
    link: function(scope) {
      scope.currentDirectory = '';
      scope.wizard = {
        visible: false,
      };
      scope.openWizard = function() {
        scope.wizard.visible = true;
        scope.fileUploads += 1;
      };
      scope.closeWizard = function() {
        scope.wizard.visible = false;
        scope.fileUploads -= 1;
      };
      scope.$watch('wizard.tableImported', function(table) {
        if (table !== undefined) {
          scope.closeWizard();
          scope.tables.unshift(table);
          scope.selected = table.id;
        }
      });
    },
  };
});
