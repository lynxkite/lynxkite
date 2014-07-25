'use strict';

angular.module('biggraph').directive('segmentation', function() {
  return {
    scope: { segmentation: '=', side: '=' },
    templateUrl: 'segmentation.html',
  };
});
