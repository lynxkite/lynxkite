// The toolbox shows the list of operation categories and the operations.
'use strict';

angular.module('biggraph').directive('operationSelector', function(/* $rootScope */) {
  return {
    restrict: 'E',
    // A lot of internals are exposed, because this directive is used both in
    // side-operation-toolbox and in project-history.
    scope: {
      boxCatalog: '=',  // (Input.) List of operation categories.
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
              icon: 'wrench',
            };
            scope.categories.push(cat);
            categories[box.categoryID] = cat;
          }
          categories[box.categoryID].ops.push(box);
        }

      });

      scope.$watch('searching && !op', function(search) {
        if (search) {
          var filter = elem.find('#filter');
          filter.focus();
          scope.searchSelection = 0;
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
        } else if (e.keyCode === 13) { // ENTER
          e.preventDefault();
          var op = angular.element(operations[scope.searchSelection]).scope().op;
          scope.clickedOp(op);
        }
      };

      scope.clickedCat = function(cat) {
        if (scope.category === cat && !scope.op) {
          scope.category = undefined;
        } else {
          scope.category = cat;
        }
        scope.searching = undefined;
        scope.op = undefined;
      };
      scope.clickedOp = function(op) {
        if (op.status.enabled) {
          scope.op = op.id;
        }
      };
      scope.searchClicked = function() {
        if (scope.searching) {
          scope.searching = undefined;
          scope.op = undefined;
        } else {
          startSearch();
        }
      };
      scope.$on('open operation search', startSearch);
      function startSearch() {
        scope.op = undefined;
        scope.category = undefined;
        scope.searching = true;
      }
    },

  };
});

