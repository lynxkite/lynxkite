'use strict';

angular.module('biggraph').directive('vertexAttribute', function() {
  return {
    scope: { attr: '=vertexAttribute', side: '=' },
    templateUrl: 'vertex-attribute.html',
    link: function(scope) {
      var type = scope.attr.typeName;
      scope.canBucket = type === 'Double' || type === 'String';
      scope.canFilter = type === 'Double' || type === 'String';
    }
  };
});
