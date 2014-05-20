'use strict';

angular.module('biggraph')
  .controller('OperationParametersCtrl', function($scope, $modalInstance, operation) {
    $scope.operation = operation;
    $scope.result = operation.parameters.map(function(p) { return p.defaultValue; });
    $scope.close = function() { $modalInstance.close($scope.result); };
  });
