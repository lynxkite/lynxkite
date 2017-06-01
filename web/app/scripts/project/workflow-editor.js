// Presents the parameters for saving an operation.
'use strict';

angular.module('biggraph').directive('workflowEditor', function(side, util) {
  return {
    restrict: 'E',
    scope: { side: '=', state: '=' },
    templateUrl: 'scripts/project/workflow-editor.html',
    link: function(scope) {
      scope.close = function() {
        scope.state.enabled = false;
        scope.state.workflow = {};
      };
      scope.save = function() {
        util.post(
          '/ajax/saveWorkflow',
          {
            workflowName: scope.state.workflow.name,
            stepsAsGroovy: scope.state.workflow.code,
            description: scope.state.workflow.description,
          }).then(function() {
            scope.close();
            scope.side.reloadAllProjects();
          });
      };
      scope.getParams = function() {
        var code = '';
        if (scope.state && scope.state.workflow && scope.state.workflow.code) {
          code = scope.state.workflow.code;
        }
        var params = code.match(/params\[['"](.*?)['"]\]/g);
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
