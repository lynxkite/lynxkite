'use strict';

// Viewer of an exportResult state.

angular.module('biggraph')
 .directive('computeStateView', function(util) {
   return {
     restrict: 'E',
     templateUrl: 'scripts/workspace/compute-state-view.html',
     scope: {
       stateId: '=',
     },
     link: function(scope) {
       scope.computed = false;
       scope.compute = function() {
         scope.computeBoxResult = util.get('ajax/getComputeBoxResult',
           {
             stateId: scope.stateId,
           }
         );
       };
       scope.computeBoxResult.then(function success() {
         scope.computed = true;
       });
     },
   };
 });
