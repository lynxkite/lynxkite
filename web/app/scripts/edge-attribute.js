// The line for an edge attribute in the project view.
'use strict';

angular.module('biggraph').directive('edgeAttribute', function(axisOptions) {
  return {
    scope: { attr: '=edgeAttribute', side: '=' },
    templateUrl: 'edge-attribute.html',
    link: function(scope) {
      axisOptions.bind(scope, scope.side, 'edge', scope.attr.title, 'axisOptions');
    },
  };
});
