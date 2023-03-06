'use strict';
import '../app';

angular.module('biggraph').directive('dirEntryIcon',
  function() {
    return {
      restrict: 'E',
      scope: {
        objectType: '='
      },
      templateUrl: 'scripts/splash/dir-entry-icon.html',
    };
  }
);
