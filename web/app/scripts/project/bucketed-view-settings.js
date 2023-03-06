// Visualization settings in bucketed view.
'use strict';
import '../app';
import './view-settings';
import templateUrl from './bucketed-view-settings.html?url';

angular.module('biggraph').directive('bucketedViewSettings', ["ViewSettings", function(ViewSettings) {
  return {
    scope: { side: '=' },
    restrict: 'E',
    templateUrl,
    link: function(scope, element) {
      new ViewSettings(scope, element);
    },
  };
}]);
