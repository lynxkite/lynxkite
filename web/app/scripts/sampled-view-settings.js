// Visualization settings in sampled view.
'use strict';

angular.module('biggraph').directive('sampledViewSettings', function(ViewSettings) {
  return {
    scope: { side: '=' },
    restrict: 'E',
    templateUrl: 'sampled-view-settings.html',
    link: function(scope, element) {
      new ViewSettings(scope, element);
    },
  };
});
