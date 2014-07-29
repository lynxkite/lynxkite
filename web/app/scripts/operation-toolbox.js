'use strict';

angular.module('biggraph').directive('operationToolbox', function($resource, deepWatch) {
  return {
    restrict: 'E',
    scope: { ops: '=', side: '=' },
    replace: true,
    templateUrl: 'operation-toolbox.html',
    link: function(scope) {
      var ApplyOp = $resource('/ajax/applyOp');
      var colors = ['yellow', 'orange', 'green', 'blue'];
      deepWatch(scope, 'ops', function() {
        scope.categories = [];
        for (var i = 0; i < scope.ops.categories.length; ++i) {
          var data = scope.ops.categories[i];
          var cat = { title: data.title, ops: data.ops };
          cat.icon = cat.title[0];
          cat.color = colors[i];
          scope.categories.push(cat);
        }
      });
      function close() {
        scope.active = undefined;
      }
      function open(cat) {
        scope.active = cat;
      }
      scope.clicked = function(cat) {
        if (scope.active === cat) {
          close();
        } else {
          open(cat);
        }
      };
      scope.execute = function(op) {
        // Placeholder.
        // TODO: Actually execute the operation.
        ApplyOp.post(op);
      };
    },
  };
});
