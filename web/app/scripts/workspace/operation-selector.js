// The toolbox shows the list of operation categories and the operations.
// Operation can be dragged to the workspace drawing board to create boxes.
'use strict';

angular.module('biggraph').directive('operationSelector', function(util) {
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
          if (!(box.categoryID in categories)) {
            var cat = {
              title: box.categoryID,
              ops: [],
              color: 'blue',
            };
            scope.categories.push(cat);
            categories[box.categoryID] = cat;
          }
          categories[box.categoryID].ops.push(box);
        }

      });

      scope.$watch('mode === "search" && !op', function(search) {
        if (search) {
          var filter = elem.find('#filter');
          filter.focus();
          scope.searchSelection = 0;
        }
      });

      scope.filterKey = function(e) {
        if (scope.mode !== 'search') { return; }
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
        }
      };

      scope.clickedCat = function(cat) {
        if (scope.category === cat) {
          scope.category = undefined;
        } else {
          scope.category = cat;
        }
        scope.mode = undefined;
      };

      scope.searchClicked = function() {
        if (scope.mode === 'searching') {
          scope.mode = undefined;
        } else {
          startSearch();
        }
      };

      scope.customClicked = function() {
        if (scope.mode === 'custom') {
          scope.mode = undefined;
        } else {
          scope.mode = 'custom';
          scope.customBoxCatalog = util.nocache('/ajax/customBoxCatalog');
        }
        scope.category = undefined;
      };

      scope.reportRequestError = util.reportRequestError;

      scope.$on('open operation search', startSearch);
      function startSearch() {
        scope.category = undefined;
        scope.mode = 'searching';
      }
    },
  };
});

