// Presents the parameters for saving an operation.
'use strict';

angular.module('biggraph').directive('workflowSaver', function(util) {
  return {
    restrict: 'E',
    scope: { code: '=', visible: '=', side: '=' },
    templateUrl: 'workflow-saver.html',
    link: function(scope) {
      scope.name = '';
      scope.description = '';
      scope.cancel = function() {
        scope.visible = false;
      };
      scope.save = function() {
        util.post('/ajax/saveWorkflow', {
          workflowName: scope.name,
          stepsAsJSON: scope.code,
          description: scope.description,
        }).then(function(success) {
          if (success) {
            scope.visible = false;
          }
        });
      };
    }
  };
});
