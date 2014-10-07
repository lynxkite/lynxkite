'use strict';

angular.module('biggraph').directive('segmentation', function($timeout) {
  return {
    scope: { segmentation: '=', side: '=' },
    templateUrl: 'segmentation.html',
    link: function(scope, element) {
      scope.toggleRenaming = function() {
        scope.renaming = !scope.renaming;
        scope.newName = scope.segmentation.name;
        // Focus #renameBox once it has appeared.
        $timeout(function() { element.find('#renameBox').focus(); });
      };
    },
  };
});
