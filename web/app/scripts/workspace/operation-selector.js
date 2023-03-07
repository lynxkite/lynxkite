// The toolbox shows the list of operation categories and the operations.
// Operation can be dragged to the workspace drawing board to create boxes.
import '../app';
import Fuse from 'fuse.js';
import templateUrl from './operation-selector.html?url';

angular.module('biggraph').directive('operationSelector', ['$timeout', '$rootScope', function($timeout, $rootScope) {
  return {
    restrict: 'E',
    scope: {
      ondrag: '&',
      onopen: '&',
      boxCatalog: '=', // (Input.) List of available boxes.
    },
    templateUrl,

    link: function(scope, elem) {
      scope.editMode = true;
      scope.categories = [];

      scope.$watch('boxCatalog.$resolved', function() {

        scope.categories = [];
        if (!scope.boxCatalog || !scope.boxCatalog.$resolved) {
          return;
        }
        scope.categories = scope.boxCatalog.categories;
        scope.boxes = [];

        const categoryMap = {};
        let i;
        for (i = 0; i < scope.categories.length; ++i) {
          const cat = scope.categories[i];
          cat.ops = [];
          categoryMap[cat.title] = cat;
        }
        for (i = 0; i < scope.boxCatalog.boxes.length; ++i) {
          const box = scope.boxCatalog.boxes[i];
          if (!(box.categoryId in categoryMap)) {
            continue;
          }
          categoryMap[box.categoryId].ops.push(box);
          scope.boxes.push(box);
        }
      });

      scope.filterKey = function(e) {
        if (!scope.searching || scope.op) { return; }
        const operations = elem.find('.operation');
        if (e.keyCode === 38) { // UP
          e.preventDefault();
          scope.searchSelection -= 1;
          if (scope.searchSelection >= operations.length) {
            scope.searchSelection = operations.length - 1;
          }
          if (scope.searchSelection < 0) {
            scope.searchSelection = 0;
          }
        } else if (e.keyCode === 40) { // DOWN
          e.preventDefault();
          scope.searchSelection += 1;
          if (scope.searchSelection >= operations.length) {
            scope.searchSelection = operations.length - 1;
          }
        } else if (e.keyCode === 27) { // ESCAPE
          scope.searching = undefined;
          scope.op = undefined;
        } else if (e.keyCode === 13) { // ENTER
          const selectedBox = scope.filterAndSort(
            scope.boxes, scope.opFilter)[scope.searchSelection];
          $rootScope.$broadcast('create box under mouse', selectedBox.operationId);
        }
      };

      scope.clickedCat = function(cat) {
        if (scope.category === cat && !scope.op) {
          scope.category = undefined;
        } else {
          scope.category = cat;
          scope.currentCatOps = cat.ops;
        }
        scope.searching = undefined;
        scope.op = undefined;
        scope.onopen();
      };
      scope.searchClicked = function() {
        if (scope.searching) {
          scope.searching = undefined;
          scope.op = undefined;
        } else {
          startSearch();
        }
      };

      scope.localOndrag = function(op, event) {
        scope.searching = undefined;
        scope.lastCat = scope.category;
        scope.category = undefined;
        scope.ondrag({ op: op, $event: event });
      };

      scope.filterAndSort = function(boxes, opFilter) {
        if (opFilter) {
          // Case insensitive by default.
          const options = {
            shouldSort: true,
            threshold: 0.4,
            location: 0,
            distance: 100,
            minMatchCharLength: 1,
            keys: [ 'operationId' ]
          };
          const fuse = new Fuse(boxes, options);
          return fuse.search(opFilter).map(r => r.item);
        } else {
          return boxes;
        }
      };
      scope.$on('open operation search', startSearch);
      function startSearch() {
        scope.op = undefined;
        scope.category = undefined;
        scope.searching = true;
        scope.opFilter = '';
        scope.searchSelection = 0;
        $timeout(function() { elem.find('#filter').focus(); });
      }
    },

  };
}]);
