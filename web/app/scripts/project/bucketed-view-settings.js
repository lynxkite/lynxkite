// Visualization settings in bucketed view.
'use strict';
import '../app';
import './view-settings';

angular.module('biggraph').directive('bucketedViewSettings', function(ViewSettings) {
  return {
    scope: { side: '=' },
    restrict: 'E',
    templateUrl: 'scripts/project/bucketed-view-settings.template',
    link: function(scope, element) {
      new ViewSettings(scope, element);
    },
  };
});
