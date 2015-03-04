'use strict';

angular.module('biggraph').directive('projectHistory', function(util) {
  return {
    restrict: 'E',
    scope: { show: '=', side: '=' },
    templateUrl: 'project-history.html',
    link: function(scope) {
      scope.$watch('show', function(show) {
        if (show) {
          scope.history = util.nocache('/ajax/getHistory', {
            project: scope.side.state.projectName,
          });
        }
      });

      function update() {
        scope.unsaved = false;
        var history = scope.history;
        if (history && history.$resolved && !history.$error) {
          for (var i = 0; i < history.steps.length; ++i) {
            var step = history.steps[i];
            watchStep(step);
          }
        }
      }
      scope.$watch('history', update);
      scope.$watch('history.$resolved', update);

      function watchStep(step) {
        util.deepWatch(
          scope,
          function() { return step; },
          function(after, before) {
            if (after === before) { return; }
            step.unsaved = true;
            scope.unsaved = true;
          });
      }

      function alternateHistory() {
        var requests = [];
        var steps = scope.history.steps;
        for (var i = 0; i < steps.length; ++i) {
          requests.push(steps[i].request);
        }
        return {
          project: scope.side.state.projectName,
          skips: scope.history.skips,
          requests: requests,
        };
      }

      scope.validate = function() {
        scope.modified = true;
        scope.history = util.get('/ajax/validateHistory', alternateHistory());
      };

      scope.saveAs = function(newName) {
        util.post('/ajax/saveHistory', {
          project: newName,
          history: alternateHistory(),
        }).then(function() {
          scope.side.reload();
        });
      };
    },
  };
});
