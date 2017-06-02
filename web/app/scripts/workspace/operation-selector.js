// The toolbox shows the list of operation categories and the operations.
// Operation can be dragged to the workspace drawing board to create boxes.
'use strict';

angular.module('biggraph').directive('operationSelector', function($timeout) {
  return {
    restrict: 'E',
    scope: {
      boxCatalog: '=',  // (Input.) List of available boxes.
    },
    templateUrl: 'scripts/workspace/operation-selector.html',

    link: function(scope, elem) {
      scope.editMode = true;
      scope.categories = [];

      scope.$watch('boxCatalog.$resolved', function() {

        scope.categories = [];
        if (!scope.boxCatalog || !scope.boxCatalog.$resolved) {
          return;
        }
        scope.boxes = scope.boxCatalog.boxes;

        var categories = {};
        for (var i = 0; i < scope.boxes.length; ++i) {
          var box = scope.boxes[i];
          if (!(box.categoryId in categories)) {
            var cat = {
              title: box.categoryId,
              ops: [],
              color: 'blue',
            };
            scope.categories.push(cat);
            categories[box.categoryId] = cat;
          }
          categories[box.categoryId].ops.push(box);
        }

      });

      scope.filterKey = function(e) {
        if (!scope.searching || scope.op) { return; }
        var operations = elem.find('.operation');
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
      };
      scope.searchClicked = function() {
        if (scope.searching) {
          scope.searching = undefined;
          scope.op = undefined;
        } else {
          startSearch();
        }
      };
      scope.closeCurrentCatOrSearch = function() {
        scope.searching = undefined;
        scope.lastCat = scope.category;
        scope.category = undefined;
      };
      scope.openLastCatOrSearch = function() {
        if (scope.lastCat) {
          scope.category = scope.lastCat;
        } else {
          scope.searching = true;
          scope.opFilter = '';
        }
      };
      scope.filter = function(boxes, opFilter) {
        /* global Fuse */
        var options = {
          shouldSort: true,
          threshold: 0.6,
          location: 0,
          distance: 100,
          maxPatternLength: 32,
          minMatchCharLength: 1,
          keys: [ "operationId" ]
        };
        var fuse = new Fuse(boxes, options);
        return fuse.search(opFilter);
      };
      scope.$on('open operation search', startSearch);
      function startSearch() {
        scope.op = undefined;
        scope.category = undefined;
        scope.searching = true;
        scope.searchSelection = 0;
        $timeout(function() { elem.find('#filter').focus(); });
      }
    },

  };
});
