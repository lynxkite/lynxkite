// Presents the parameters for saving an operation.
'use strict';

angular.module('biggraph').directive('workflowSaver', function(side, util) {
  return {
    restrict: 'E',
    scope: { side: '=', initialName: '@' },
    templateUrl: 'workflow-saver.html',
    link: function(scope) {
      var loadWorkflow = function() {
        var id = scope.state.id;
        scope.state.id = undefined;
        util.get(
          '/ajax/workflow',
          {
            workflowName: id
          }).then(function(response) {  // success
            function getVisibleName(id) {
              return id.split('/')[1];
            }
            function getVisibleDescription(description) {
              return description.split('<p>').slice(2).join('<p>');
            }
            scope.state.code = response.stepsAsGroovy;
            scope.state.name = getVisibleName(id);
            scope.state.description = getVisibleDescription(response.description);
          },
          function() {  // failure
            scope.cancel();
          });
      };
      scope.state = scope.side.state.workflow;
      if (scope.state.id !== undefined) {
        loadWorkflow();
      }
      scope.cancel = function() {
        scope.state.enabled = false;
        scope.state.code = '';
        scope.state.description = '';
        scope.state.name = '';
      };
      scope.save = function() {
        util.post(
          '/ajax/saveWorkflow',
          {
            workflowName: scope.state.name,
            stepsAsGroovy: scope.state.code,
            description: scope.state.description,
          }).then(function() {
            scope.cancel();
            scope.side.reloadAllProjects();
          });
      };
      scope.getParams = function() {
        var params = scope.state.code.match(/params\[['"](.*?)['"]\]/g);
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
