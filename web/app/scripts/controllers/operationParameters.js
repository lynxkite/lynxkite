'use strict';

angular.module('biggraph')
  .controller('OperationParametersCtrl', function($scope, $modalInstance, operation) {
    $scope.operation = operation;
    $scope.result = {};
    operation.parameters.map(function(p) { $scope.result[p.id] = p.defaultValue; });
    $scope.close = function() {
      var result = $scope.result;
      $scope.result = {}; // Disengage bindings.
      // Replace arrays with comma-separated lists.
      operation.parameters.forEach(function(p) {
        if (p.kind.indexOf('multi-') === 0 && typeof result[p.id] === 'object') {
          result[p.id] = result[p.id].join(',');
        }
      });
      $modalInstance.close(result);
    };
  });
