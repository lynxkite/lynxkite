// Presents the parameters for saving an operation.
'use strict';

angular.module('biggraph').directive('workflowSaver', function(util) {
  return {
    restrict: 'E',
    scope: { code: '=', mode: '=', side: '=' },
    templateUrl: 'workflow-saver.html',
    link: function(scope) {
      scope.name = '';
      scope.description = '';
      scope.cancel = function() {
        scope.mode.enabled = false;
      };
      scope.save = function() {
        util.post(
          '/ajax/saveWorkflow',
          {
            workflowName: scope.name,
            stepsAsGroovy: scope.code,
            description: scope.description,
          },
          function() {
            scope.mode.enabled = false;
            scope.side.reloadAllProjects();
          });
      };
      scope.getParams = function() {
        if (!scope.code) { return []; }
        var params = scope.code.match(/params\[['"](.*?)['"]\]/g);
        if (!params) { return []; }
        var uniques = [];
        for (var i = 0; i < params.length; ++i) {
          var p = params[i].slice(8, -2);
          if (uniques.indexOf(p) === -1) {
            uniques.push(p);
          }
        }
        uniques.sort();
        return uniques;
      };
    }
  };
});
