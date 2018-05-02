// The modal dialog for generated Python code
'use strict';

angular.module('biggraph').controller('PythonCodeCtrl', function($scope, $modalInstance, code) {
  $scope.code = code;

  $scope.close = function() {
    $modalInstance.dismiss('close');
  };
});
