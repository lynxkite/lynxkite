// The toolbox shows the list of operation categories and the operations.
'use strict';

angular.module('biggraph').directive('operationToolbox', function(hotkeys) {
  return {
    restrict: 'E',
    // A lot of internals are exposed, because this directive is used both in
    // side-operation-toolbox and in project-history.
    scope: {
      categories: '=',  // (Input.) List of operation categories.
      op: '=?',  // (Input/output.) Currently selected operation's id (if any).
      params: '=?',  // (Input/output.) Currently set operation parameters.
      category: '=?',  // (Input/output.) Currently selected category (if any).
      searching: '=?',  // (Input/output.) Whether operation search is active.
      applying: '=',  // (Input.) Whether an operation is just being submitted.
    },
    templateUrl: 'operation-toolbox.html',
    link: function(scope, elem) {
      scope.$watch('categories', function(cats) {
        // The complete list, for searching.
        scope.allOps = [];
        for (var i = 0; i < cats.length; ++i) {
          scope.allOps = scope.allOps.concat(cats[i].ops);
        }
      });
      scope.$watch('searching && !op', function(search) {
        if (search) {
          var filter = elem.find('#filter')[0];
          filter.focus();
          // Here we rely on the search page living in a separate scope.
          // This happens because it is inside an ng-if, but this is a bit sneaky.
          // The point is that these hotkeys will be automatically unregistered when
          // this scope is destroyed.
          var searchKeys = hotkeys.bindTo(angular.element(filter).scope());
          scope.searchSelection = 0;
          searchKeys.add({ combo: 'up', allowIn: ['INPUT'], callback: function(e) {
            e.preventDefault();
            scope.searchSelection -= 1;
            var count = elem.find('.operation').length;
            if (scope.searchSelection >= count) {
              scope.searchSelection = count - 1;
            }
            if (scope.searchSelection < 0) {
              scope.searchSelection = 0;
            }
          }});
          searchKeys.add({ combo: 'down', allowIn: ['INPUT'], callback: function(e) {
            e.preventDefault();
            scope.searchSelection += 1;
            var count = elem.find('.operation').length;
            if (scope.searchSelection >= count) {
              scope.searchSelection = count - 1;
            }
          }});
          searchKeys.add({ combo: 'enter', allowIn: ['INPUT'], callback: function(e) {
            e.preventDefault();
            var elems = elem.find('.operation');  // Rely on Angular's filter.
            var op = angular.element(elems[scope.searchSelection]).scope().op;
            scope.clickedOp(op);
          }});
        }
      });

      scope.findColor = function(opId) {
        var op = scope.findOp(opId);
        for (var i = 0; i < scope.categories.length; ++i) {
          var cat = scope.categories[i];
          if (op.category === cat.title) {
            return cat.color;
          }
        }
        console.error('Could not find category for', opId);
        return 'yellow';
      };

      scope.findOp = function(opId) {
        for (var i = 0; i < scope.categories.length; ++i) {
          for (var j = 0; j < scope.categories[i].ops.length; ++j) {
            var op = scope.categories[i].ops[j];
            if (opId === op.id) {
              return op;
            }
          }
        }
        return undefined;
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
          scope.params = {};
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
