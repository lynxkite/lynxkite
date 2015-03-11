'use strict';

angular.module('biggraph').directive('operationToolbox', function($rootScope, hotkeys) {
  return {
    restrict: 'E',
    scope: { side: '=' },
    replace: true,
    templateUrl: 'operation-toolbox.html',
    link: function(scope, elem) {
      if (scope.side.primary) {  // Set up hotkeys on the primary project only.
        var hk = hotkeys.bindTo(scope);
        hk.add({ combo: '/', description: 'Find operation', callback: function(e) {
          e.preventDefault();  // Do not type "/".
          startSearch();
        }});
        hk.add({ combo: 'esc', allowIn: ['INPUT'], callback: function() {
          if (scope.op) {
            scope.op = undefined;
          } else if (scope.searching) {
            scope.searching = undefined;
          } else if (scope.active) {
            scope.active = undefined;
          }
        }});
        scope.$watch('side.project.opCategories', function(cats) {
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
      }

      scope.$watch('active || searching', function(open) {
        if (open) {
          $rootScope.$broadcast('close all the other operation-toolboxes', scope);
        }
      });
      scope.$on('close all the other operation-toolboxes', function(e, source) {
        if (scope !== source) {
          scope.op = undefined;
          scope.active = undefined;
          scope.searching = undefined;
        }
      });

      scope.clickedCat = function(cat) {
        if (scope.active === cat && !scope.op) {
          scope.active = undefined;
        } else {
          scope.active = cat;
        }
        scope.searching = undefined;
        scope.op = undefined;
      };
      scope.clickedOp = function(op) {
        if (op.status.enabled) {
          scope.op = op;
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
      function startSearch() {
        scope.op = undefined;
        scope.active = undefined;
        scope.searching = true;
      }
    },
  };
});
