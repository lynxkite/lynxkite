// Shows whether something has been computed already.
import '../app';
import templateUrl from './compute-progress.html?url';

angular.module('biggraph').directive('computeProgress', function() {
  return {
    scope: { computeProgress: '=model' },
    templateUrl,
  };
});
