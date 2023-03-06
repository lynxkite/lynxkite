// Visualization settings in sampled view.
'use strict';
import '../app';
import './view-settings';

angular.module('biggraph').directive('sampledViewSettings', function(ViewSettings) {
  return {
    scope: { side: '=' },
    restrict: 'E',
    templateUrl: 'scripts/project/sampled-view-settings.html',
    link: function(scope, element) {
      new ViewSettings(scope, element);
    },
  };
});
