// The entry for a segmentation in the project view.
'use strict';

angular.module('biggraph').directive('segmentation', function() {
  return {
    scope: { segmentation: '=', side: '=' },
    templateUrl: 'segmentation.html',
  };
});
