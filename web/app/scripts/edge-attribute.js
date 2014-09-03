'use strict';

angular.module('biggraph').directive('edgeAttribute', function() {
  return {
    scope: { attr: '=edgeAttribute', side: '=' },
    templateUrl: 'edge-attribute.html',
    link: function(scope) {
      var type = scope.attr.typeName;
      scope.canBucket = type === 'Double' || type === 'String';
      scope.canFilter = type === 'Double' || type === 'String';
    }
  };
});
