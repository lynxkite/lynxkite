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
          function() { return step.request; },
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

      // Performs a light validation in the browser, returning "ok" if it passes,
      // otherwise an error string.
      scope.localValidationResult = function() {
        var steps = scope.history.steps;
        for (var i = 0; i < steps.length; ++i) {
          if (steps[i].request.op.id === undefined) {
            return 'an operation is unset';
          }
        }
        return 'ok';
      };

      scope.validate = function() {
        scope.remoteChanges = true;
        scope.history = util.nocache('/ajax/validateHistory', alternateHistory());
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

      scope.findCategory = function(cats, req) {
        for (var i = 0; i < cats.length; ++i) {
          for (var j = 0; j < cats[i].ops.length; ++j) {
            var op = cats[i].ops[j];
            if (req.op.id === op.id) {
              return cats[i];
            }
          }
        }
        return undefined;
      };

      function findOp(cats, opId) {
        for (var i = 0; i < cats.length; ++i) {
          for (var j = 0; j < cats[i].ops.length; ++j) {
            var op = cats[i].ops[j];
            if (opId === op.id) {
              return op;
            }
          }
        }
        return undefined;
      }

      function opNamesForSteps(steps) {
        var names = [];
        for (var i = 0; i < steps.length; ++i) {
          var step = steps[i];
          var op = findOp(step.opCategoriesBefore, step.request.op.id);
          if (op) {
            names.push(findOp(step.opCategoriesBefore, step.request.op.id).title);
          }
        }
        return names;
      }

      scope.listLocalChanges = function() {
        var changed = opNamesForSteps(scope.history.steps.filter(
              function(step) { return step.localChanges; }));
        if (changed.length === 0) {
          return '';  // No name for the changed step.
        }
        var has = changed.length === 1 ? 'has' : 'have';
        return '(' + changed.join(', ') + ' ' + has + ' changed)';
      };

      scope.listInvalidSteps = function() {
        var invalids = opNamesForSteps(scope.history.steps.filter(
              function(step) { return !step.status.enabled; }));
        if (invalids.length === 0) {
          return '';  // No name for the invalid step.
        }
        var is = invalids.length === 1 ? 'is' : 'are';
        return '(' + invalids.join(', ') + ' ' + is + ' invalid)';
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

      // Insert new operation.
      scope.insertBefore = function(step, seg) {
        var pos = scope.history.steps.indexOf(step);
        scope.history.steps.splice(pos, 0, blankStep(seg));
        scope.validate();
      };
      scope.insertAfter = function(step, seg) {
        var pos = scope.history.steps.indexOf(step);
        scope.history.steps.splice(pos + 1, 0, blankStep(seg));
        scope.validate();
      };

      function blankStep(seg) {
        var project = scope.side.state.projectName;
        if (seg !== undefined) {
          project = seg.fullName;
        }
        return {
          request: {
            project: project,
            op: {
              id: 'No-operation',
              parameters: {},
            },
          },
          status: {
            enabled: false,
            disabledReason: 'Fetching valid operations...',
          },
          segmentationsBefore: [],
          segmentationsAfter: [],
          opCategoriesBefore: [],
        };
      }
    },
  };
});
