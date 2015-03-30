'use strict';

angular.module('biggraph').directive('scalar', function() {
  return {
    scope: { scalar: '=', value: '=', side: '=' },
    templateUrl: 'scalar.html',
    link: function(scope) {
      scope.$watch('scalar', function() {
        console.log(scope.scalar.typeName);
        var isSavedStatus =
          (scope.scalar.typeName === 'com.lynxanalytics.biggraph.controllers.UIStatus');
        scope.asSavedStatus = isSavedStatus;
        scope.asValue = !isSavedStatus;
      });
      scope.loadStatus = function() {
        scope.side.updateFromBackendJson(scope.value.string);
      };
    },
  };
});
