'use strict';

angular.module('biggraph').directive('vertexAttribute', function() {
  return {
    scope: { attr: '=vertexAttribute', side: '=' },
    templateUrl: 'vertex-attribute.html',
  };
});
