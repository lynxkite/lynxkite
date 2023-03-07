// Visualization settings in sampled view.
import '../app';
import './view-settings';
import templateUrl from './sampled-view-settings.html?url';

angular.module('biggraph').directive('sampledViewSettings', ['ViewSettings', function(ViewSettings) {
  return {
    scope: { side: '=' },
    restrict: 'E',
    templateUrl,
    link: function(scope, element) {
      new ViewSettings(scope, element);
    },
  };
}]);
