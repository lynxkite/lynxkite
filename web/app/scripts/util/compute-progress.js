// Shows whether something has been computed already.
'use strict';
import '../app';

angular.module('biggraph').directive('computeProgress', function() {
  return {
    scope: { computeProgress: '=model' },
    templateUrl: 'scripts/util/compute-progress.html',
  };
});
