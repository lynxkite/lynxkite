// Presents the parameters for saving an operation.
'use strict';

angular.module('biggraph').directive('workflowSaver', function(side, util) {
  return {
    restrict: 'E',
    scope: { side: '=', state: '=' },
    templateUrl: 'workflow-saver.html',
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
        function getCode() {
          if (scope.state && scope.state.workflow && scope.state.workflow.code) {
            return scope.state.workflow.code;
          } else {
            return '';
          }
        }
        var params = getCode().match(/params\[['"](.*?)['"]\]/g);
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
