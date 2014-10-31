'use strict';

angular.module('biggraph').directive('parent', function() {
  return {
    scope: { parent: '=', side: '=' },
    templateUrl: 'parent.html',
  };
});
