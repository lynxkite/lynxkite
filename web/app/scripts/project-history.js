'use strict';

angular.module('biggraph').directive('projectHistory', function(util) {
  return {
    restrict: 'E',
    scope: { show: '=', side: '=' },
    templateUrl: 'project-history.html',
    link: function(scope) {
      scope.$watch('show', getHistory);
      scope.$watch('side.state.projectName', getHistory);
      function getHistory() {
        if (!scope.show) { return; }
        scope.remoteChanges = false;
        scope.history = util.nocache('/ajax/getHistory', {
          project: scope.side.state.projectName,
        });
      }

      function update() {
        scope.localChanges = false;
        scope.valid = true;
        var history = scope.history;
        if (history && history.$resolved && !history.$error) {
          for (var i = 0; i < history.steps.length; ++i) {
            var step = history.steps[i];
            if (!step.status.enabled) {
              scope.valid = false;
            }
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
            step.localChanges = true;
            scope.localChanges = true;
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
        scope.remoteChanges = true;
        scope.history = util.get('/ajax/validateHistory', alternateHistory());
      };

      scope.saveAs = function(newName) {
        util.post('/ajax/saveHistory', {
          newProject: newName,
          history: alternateHistory(),
        }, function() {
          scope.side.state.projectName = newName;
        });
      };

      // Returns "on <segmentation name>" if the project is a segmentation.
      scope.onProject = function(name) {
        var path = util.projectPath(name);
        // The project name is already at the top.
        path.shift();
        return path.length === 0 ? '' : 'on ' + path.join('&raquo;');
      };

      scope.reportError = function() {
        util.reportRequestError(scope.history);
      };

      // Confirm leaving the history page if changes have been made.
      scope.$watch('show && (localChanges || remoteChanges)', function(changed) {
        scope.changed = changed;
        window.onbeforeunload = !changed ? null : function(e) {
          e.returnValue = 'Your history changes are unsaved.';
          return e.returnValue;
        };
      });
      scope.closeHistory = function() {
        if (!scope.changed || window.confirm('Discard history changes?')) {
          scope.side.showHistory = false;
        }
      };
      scope.closeProject = function() {
        if (!scope.changed || window.confirm('Discard history changes?')) {
          scope.side.close();
        }
      };
    },
  };
});
