'use strict';

angular.module('biggraph').directive('operationToolbox', function(util) {
  return {
    restrict: 'E',
    scope: { side: '=' },
    replace: true,
    templateUrl: 'operation-toolbox.html',
    link: function(scope) {
      var colors = ['yellow', 'orange', 'green', 'blue'];
      util.deepWatch(scope, 'side.data.opCategories', function(categories) {
        scope.categories = [];
        for (var i = 0; i < categories.length; ++i) {
          var data = categories[i];
          var cat = { title: data.title, ops: data.ops };
          cat.icon = cat.title[0];
          cat.color = colors[i % colors.length];
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
    },
  };
});
