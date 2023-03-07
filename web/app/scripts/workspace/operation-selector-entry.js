// An entry in the operation selector list. Supports dragging operations from here into
// the workspace drawing board.
import '../app';
import templateUrl from './operation-selector-entry.html?url';

angular.module('biggraph').directive('operationSelectorEntry', function() {
  return {
    restrict: 'E',
    scope: {
      ondrag: '&', // The function to handle dragging.
      op: '=', // The actual underlying operation.
      name: '=', // Name to display on the UI.
    },
    templateUrl,
  };
});
