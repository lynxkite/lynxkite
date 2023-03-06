// UI for defining wizard steps.
'use strict';
import '../app';
import '../util/util';

angular.module('biggraph').directive('wizardStepsParameter', function(util) {
  return {
    restrict: 'E',
    scope: {
      workspace: '=',
      model: '=',
    },
    templateUrl: 'scripts/operation/wizard-steps-parameter.html',
    link: function(scope) {
      scope.$watch('model', function(model) {
        scope.steps = JSON.parse(model);
      });
      util.deepWatch(scope, 'steps', function(steps) {
        scope.model = JSON.stringify(steps);
      });

      scope.add = function() {
        scope.steps.push({ title: '', description: '', box: '', popup: '' });
      };

      scope.up = function(index) {
        if (index <= 0) { return; }
        const s = scope.steps[index];
        scope.steps[index] = scope.steps[index - 1];
        scope.steps[index - 1] = s;
      };
      scope.down = function(index) {
        if (index >= scope.steps.length - 1) { return; }
        const s = scope.steps[index];
        scope.steps[index] = scope.steps[index + 1];
        scope.steps[index + 1] = s;
      };

      scope.discard = function(index) {
        scope.steps.splice(index, 1);
      };

      scope.boxes = function() {
        return scope.workspace.boxes.map(b => b.instance.id);
      };

      scope.popups = function(id) {
        const box = getBox(id);
        return box ? ['parameters', ...box.outputs.map(p => p.id)] : [];
      };

      function getBox(id) {
        return scope.workspace.getBox(id);
      }

      scope.defaultTitle = function(step) {
        const box = getBox(step.box);
        return box ? box.instance.operationId : '<name of box>';
      };
    },
  };
});
