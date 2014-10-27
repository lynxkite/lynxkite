'use strict';

angular.module('biggraph').directive('edgeAttribute', function() {
  return {
    scope: { attr: '=edgeAttribute', side: '=' },
    templateUrl: 'edge-attribute.html',
    link: function(scope) {
      scope.logarithmic = function(log) {  // Setter/getter.
        var ao = scope.side.axisOptions('edge', scope.attr.title);
        if (log !== undefined) {
          ao.logarithmic = log;
          scope.side.state.axisOptions.edge[scope.attr.title] = ao;
        }
        return ao.logarithmic;
      };
    },
  };
});
