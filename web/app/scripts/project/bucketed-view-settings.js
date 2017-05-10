// Visualization settings in bucketed view.
'use strict';

angular.module('biggraph').directive('bucketedViewSettings', function(ViewSettings) {
  return {
    scope: { side: '=' },
    restrict: 'E',
    templateUrl: 'scripts/project/bucketed-view-settings.html',
    link: function(scope, element) {
      new ViewSettings(scope, element);
    },
  };
});
