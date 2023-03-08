import '../app';
import templateUrl from './dir-entry-icon.html?url';

angular.module('biggraph').directive('dirEntryIcon',
  function() {
    return {
      restrict: 'E',
      scope: {
        objectType: '='
      },
      templateUrl,
    };
  }
);
