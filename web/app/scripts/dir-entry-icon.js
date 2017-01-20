'use strict';

angular.module('biggraph').directive('dirEntryIcon',
  function() {
    return {
      restrict: 'E',
      scope: {
        objectType: '='
      },
      templateUrl: 'dir-entry-icon.html',
    };
  }
);
