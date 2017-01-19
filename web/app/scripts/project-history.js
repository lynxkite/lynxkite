// The project history viewer/editor.
'use strict';

angular.module('biggraph').directive('projectHistory',
function(util, $timeout, removeOptionalDefaults) {
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
        scope.updatedHistory = undefined;
      }

      function update() {
        scope.localChanges = false;
        scope.valid = true;
        var history = scope.history;
        scope.historyBackup = angular.copy(scope.history);
        if (history && history.$resolved && !history.$error) {
          for (var i = 0; i < history.steps.length; ++i) {
            setupHistoryStep(i);
          }
        }
      }
      function setupHistoryStep(i) {
        var history = scope.history;
        var step = history.steps[i];
        step.localChanges = false;
        step.editable = scope.valid;
        if ((step.checkpoint === undefined) && !step.status.enabled) {
          scope.valid = false;
        }
        watchStep(i, step);
      }
      scope.discardChanges = function() {
        scope.history = scope.historyBackup;
        update();
      };

      scope.$watch('history', update);
      scope.$watch('history.$resolved', update);
      scope.$on('apply operation', validate);

      function watchStep(index, step) {
        util.deepWatch(
          scope,
          function() { return step.request; },
          function(after, before) {
            if (after === before) { return; }

            // If all what happened is defining some previously undefined parameters then
            // we don't take this as a change. This kind of change is always done by
            // operation.js (not by the user). It basically sets the defaults provided by the
            // backend for all undefined values. We assume having the default value is equivalent
            // with not having the parameters defined for these operations.
            // For why this is necessary, see: https://github.com/biggraph/biggraph/issues/1985
            var beforeAllDefined = angular.copy(before);
            var beforeParams = beforeAllDefined.op.parameters;
            var afterParams = after.op.parameters;
            angular.forEach(afterParams, function(value, param) {
              if (beforeParams[param] === undefined) {
                beforeParams[param] = value;
              }
            });
            if (angular.equals(after, beforeAllDefined)) { return; }

            step.localChanges = true;
            scope.localChanges = true;
            clearCheckpointsFrom(index);
          });
      }

      function clearCheckpointsFrom(index) {
        // Steps after a change cannot use checkpoints.
        // This is visually communicated as well.
        var steps = scope.history.steps;
        for (var i = index; i < steps.length; ++i) {
          steps[i].checkpoint = undefined;
        }
        for (i = 0; i < steps.length; ++i) {
          if (i !== index) {
            steps[i].editable = false;
          }
        }
      }

      function alternateHistory() {
        var requests = [];
        var steps = scope.history.steps;
        var lastCheckpoint = ''; // The initial project state.
        for (var i = 0; i < steps.length; ++i) {
          var s = steps[i];
          if (requests.length === 0 && s.checkpoint !== undefined) {
            lastCheckpoint = s.checkpoint;
          } else {
            requests.push(s.request);
          }
        }
        return {
          startingPoint: lastCheckpoint,
          requests: requests,
        };
      }

      function validate() {
        // The browser may forget the page scroll position in certain conditions.
        // In particular, destroying and rebuilding the ACE editor control
        // messes up its value. Therefore we back it up here and restore it once
        // the DOM tree has stabilized.
        scope.scrollPositionBackup = window.pageYOffset;
        scope.validating = true;
        scope.updatedHistory = util.post('/ajax/validateHistory', alternateHistory());
        // The response will be evaluated in copyUpdate.
      }
      function copyUpdate() {
        if (scope.updatedHistory && scope.updatedHistory.$resolved) {
          scope.remoteChanges = true;
          scope.validating = false;
          scope.history = scope.updatedHistory;
          $timeout(function() {
            // The scrollbar position will be restored once the DOM of the document
            // is stabilized.
            if (scope.scrollPositionBackup) {
              window.scrollTo(0, scope.scrollPositionBackup);
              scope.scrollPositionBackup = undefined;
            }
          }, 0);
        }
      }
      scope.$watch('updatedHistory', copyUpdate);
      scope.$watch('updatedHistory.$resolved', copyUpdate);

      scope.saveAs = function(newName) {
        scope.saving = true;
        util.post('/ajax/saveHistory', {
          oldProject: scope.side.state.projectName,
          newProject: newName,
          history: alternateHistory(),
        }).then(function() {
          // On success.
          if (scope.side.state.projectName === newName) {
            scope.side.reload();
          } else {
            scope.side.state.projectName = newName; // Will trigger a reload.
          }
          scope.side.showHistory = false;
        }).finally(function() {
          // On completion, regardless of success.
          scope.saving = false;
        });
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

      function opNamesForSteps(steps) {
        var names = [];
        for (var i = 0; i < steps.length; ++i) {
          var op = steps[i].opMeta;
          if (op) {
            names.push(op.title);
          }
        }
        return names;
      }

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

      // Discard operation.
      scope.discard = function(step) {
        var pos = scope.history.steps.indexOf(step);
        clearCheckpointsFrom(pos);
        scope.history.steps.splice(pos, 1);
        validate();
      };

      // Insert new operation.
      scope.insertBefore = function(step, seg) {
        var pos = scope.history.steps.indexOf(step);
        clearCheckpointsFrom(pos);
        scope.history.steps.splice(pos, 0, createNewStep(seg));
        validate();
      };
      scope.insertAfter = function(step, seg) {
        var pos = scope.history.steps.indexOf(step);
        clearCheckpointsFrom(pos + 1);
        scope.history.steps.splice(pos + 1, 0, createNewStep(seg));
        validate();
      };

      // Returns the short name of the segmentation if the step affects a segmentation.
      scope.segmentationName = function(step) {
        var path = step.request.path;
        if (path.length === 0) { // This is the top-level project.
          return undefined;
        } else {
          return path.join('|');
        }
      };

      scope.enterWorkflowSaving = function() {
        var history = scope.history;
        var code = '';
        if (history && history.$resolved && !history.$error) {
          code = toGroovy(history.steps);
        }
        scope.side.unsaved.workflowEditor = {
          enabled: true,
          workflow: {
            code: code,
            name: '',
            description: ''
          }
        };
      };

      scope.togglePython = function() {
        if (scope.python) {
          scope.python = undefined;
          return;
        }
        var history = scope.history;
        if (history && history.$resolved && !history.$error) {
          scope.python = toPython(history.steps);
        }
      };

      function createNewStep(seg) {
        var path = [];
        if (seg !== undefined) {
          path = seg.name.split('|');
        }
        return {
          request: {
            path: path,
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

      scope.categoriesCallback = function(index) {
        if (scope.history.steps[index].checkpoint) {
          return util.get('/ajax/getOpCategories', {
            startingPoint: index > 0 ? scope.history.steps[index - 1].checkpoint : '',
            requests: []
          });
        } else {
          var altHist = alternateHistory();
          var totalRealHistoryLength = scope.history.steps.length - altHist.requests.length;
          var required = index - totalRealHistoryLength;
          altHist.requests = altHist.requests.slice(0, required + 1);
          return util.get('/ajax/getOpCategories', altHist);
        }
      };

      function toCodeId(name) {
        return name
          .replace(/^./, function(first) { return first.toLowerCase(); })
          .replace(/-./g, function(dashed) { return dashed[1].toUpperCase(); });
      }

      function codeQuote(str) {
        str = str.replace(/\\/g, '\\\\');
        str = str.replace(/\n/g, '\\n');
        str = str.replace(/'/g, '\\\'');
        return '\'' + str + '\'';
      }

      function toGroovy(steps) {
        var lines = [];
        for (var i = 0; i < steps.length; ++i) {
          var step = steps[i];
          var request = step.request;
          var op = step.opMeta;
          var line = [];
          line.push('project');
          for (var j = 0; j < request.path.length; ++j) {
            var seg = request.path[j];
            line.push('.segmentations[\'' + seg + '\']');
          }
          var params = removeOptionalDefaults(request.op.parameters, op);
          var paramKeys = Object.keys(params);
          paramKeys.sort();
          if (request.op.id.indexOf('workflows/') === 0) {
            var workflowName = request.op.id.split('/').slice(1).join('/');
            line.push('.runWorkflow(\'' + workflowName + '\'');
            if (params.length > 0) { line.push(', '); }
          } else {
            line.push('.' + toCodeId(request.op.id) + '(');
          }
          for (j = 0; j < paramKeys.length; ++j) {
            var k = paramKeys[j];
            var v = params[k];
            if (!k.match(/^[a-zA-Z]+$/)) {
              k = codeQuote(k);
            }
            v = codeQuote(v);
            line.push(k + ': ' + v);
            if (j !== paramKeys.length - 1) {
              line.push(', ');
            }
          }
          line.push(')');
          lines.push(line.join(''));
        }
        return lines.join('\n');
      }

      function toPython(steps) {
        var lines = [];
        var reservedwords = [
          'and', 'as', 'assert', 'break', 'class', 'continue',
          'def', 'del', 'elif', 'else', 'except', 'exec', 'finally', 'for', 'from',
          'global', 'if', 'import', 'in', 'is', 'lambda', 'not', 'or', 'pass',
          'print', 'raise', 'return', 'try', 'while', 'with', 'yield'];

        for (var i = 0; i < steps.length; ++i) {
          var step = steps[i];
          var request = step.request;
          var op = step.opMeta;
          var line = [];
          line.push('project');
          for (var j = 0; j < request.path.length; ++j) {
            var seg = request.path[j];
            line.push('.segmentation(' + codeQuote(seg) + ')');
          }
          var params = removeOptionalDefaults(request.op.parameters, op);
          var paramKeys = Object.keys(params);
          paramKeys.sort();
          line.push('.' + toCodeId(request.op.id) + '(');
          var problemParams = false;
          for (j = 0; j < paramKeys.length; ++j) {
            if (!paramKeys[j].match(/^[a-zA-Z]+$/) ||
                reservedwords.indexOf(paramKeys[j]) > -1) {
              problemParams = true;
              line.push('**{');
              break;
            }
          }
          for (j = 0; j < paramKeys.length; ++j) {
            var k = paramKeys[j];
            var v = params[k];
            if (!v.match(/^\d+(\.\d+)?$/)) {
              // Quote non-numbers.
              v = codeQuote(v);
            }
            if (problemParams) {
              line.push(codeQuote(k) + ': ' + v);
            } else {
              line.push(k + '=' + v);
            }
            if (j !== paramKeys.length - 1) {
              line.push(', ');
            }
          }
          if (problemParams) {
            line.push('}');
          }
          line.push(')');
          lines.push(line.join(''));
        }
        return lines.join('\n');
      }
    },
  };
});
