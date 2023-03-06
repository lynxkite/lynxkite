// The modal dialog for generated Python code
'use strict';
import './app';

angular.module('biggraph').controller('PythonCodeCtrl', ['$scope', '$uibModalInstance', 'code', function($scope, $uibModalInstance, code) {
  $scope.code = code;

  $scope.selectAll = function() {
    let text = angular.element('#python-code')[0];
    text.focus();
    text.select();
  };

  $scope.close = function() {
    $uibModalInstance.dismiss('close');
  };
}]);
