'use strict';

angular.module('biggraph').directive('edgeAttribute', function() {
  return {
    scope: { attr: '=edgeAttribute', side: '=' },
    templateUrl: 'edge-attribute.html',
    link: function(scope) {
    },
  };
});
