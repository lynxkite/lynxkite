'use strict';

angular.module('biggraph')
  .controller('FileDialogCtrl', function($scope, $modalInstance, question) {
    $scope.question = question;
    $scope.results = {
      targetDirPath: '',
      awsAccessKeyId: '',
      awsSecretAccessKey: ''
    };
    $scope.close = function() {
      $modalInstance.close($scope.results);
    };
  });
