// The toolbox shows the list of operation categories and the operations.
'use strict';

angular.module('biggraph').directive('operationToolbox', function($rootScope) {
  return {
    restrict: 'E',
    // A lot of internals are exposed, because this directive is used both in
    // side-operation-toolbox and in project-history.
    scope: {
      categories: '=',  // (Input.) List of operation categories.
      op: '=?',  // (Input/output.) Currently selected operation's id (if any).
      opMeta: '=?',  // (Output.) Currently selected operation's metadata.
      params: '=?',  // (Input/output.) Currently set operation parameters.
      category: '=?',  // (Input/output.) Currently selected category (if any).
      searching: '=?',  // (Input/output.) Whether operation search is active.
      applying: '=',  // (Input.) Whether an operation is just being submitted.
      editable: '=',  // (Input.) Whether the toolbox should be interactive.
      sideWorkflowEditor: '=',  // (Input/output.) The workflow editor available on this side.
      historyMode: '=',  // (Input.) Whether this toolbox is inside the history browser.
      step: '=',  // (Input.) If historyMode is true, this is the history step of the operation.
      discardStep: '&', // (Method.) For manipulating history.
      discardChanges: '&', // (Method.) For manipulating history.
      categoriesCallback: '&' // (Input.) Callback for when there is no categories or checkpoint.
    },
    templateUrl: 'operation-toolbox.html',
    link: function(scope, elem) {
      scope.editMode = !scope.historyMode;
      if (scope.historyMode) {
        scope.enterEditMode = function() {
          if (!scope.categories) {
            scope.categoriesCallback().then(
              function (result) {
                scope.categories = result.categories;
              });
          }
          scope.editMode = true;
          $rootScope.$broadcast('close all other history toolboxes', scope);
        };
        scope.discardChangesAndFinishEdit = function() {
          scope.discardChanges();
          scope.editMode = false;
          scope.categories = undefined;
        };
        scope.$watch('step.localChanges', function() {
          if (scope.step.localChanges) {
            scope.enterEditMode();
          }
        });

        scope.$on(
          'close all other history toolboxes',
          function(event, src) {
            if (src !== scope) {
              scope.editMode = false;
              scope.categories = undefined;
            }
          });
        if (scope.step.request.op.id === 'No-operation') {
          // This is a hack so that newly added operations
          // start off with their category menu open.
          scope.enterEditMode();
        }
      }

      scope.$watch('categories', function(cats) {
        // The complete list, for searching.
        scope.allOps = [];
        if (!cats) {
          return;
        }

        for (var i = 0; i < cats.length; ++i) {
          scope.allOps = scope.allOps.concat(cats[i].ops);
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

      scope.$watch('opMeta', function(op) {
        scope.opColor = 'yellow';
        if (op === undefined) {
          return;
        }

        if (op.color) {
          scope.opColor = op.color;
          return;
        }

        for (var i = 0; i < scope.categories.length; ++i) {
          var cat = scope.categories[i];
          if (op.category === cat.title) {
            scope.opColor = cat.color;
            return;
          }
        }
        /* eslint-disable no-console */
        console.error('Could not find category for', op.id);
      });

      scope.$watch('op', function(opId) {
        if (!scope.categories) {
          return;
        }

        for (var i = 0; i < scope.categories.length; ++i) {
          for (var j = 0; j < scope.categories[i].ops.length; ++j) {
            var op = scope.categories[i].ops[j];
            if (opId === op.id) {
              scope.opMeta = op;
              return;
            }
          }
        }
        scope.opMeta = undefined;
      });

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

angular.module('biggraph').factory('removeOptionalDefaults', function() {
  return function(params, op) {
    params = angular.extend({}, params); // Shallow copy.
    for (var i = 0; i < op.parameters.length; ++i) {
      var param = op.parameters[i];
      if (!param.mandatory && params[param.id] === param.defaultValue) {
        delete params[param.id];
      }
    }
    return params;
  };
});
