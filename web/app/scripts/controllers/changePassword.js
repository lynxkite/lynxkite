// The "/changepassword" page
'use strict';

angular.module('biggraph')
  .controller('ChangePasswordCtrl', function ($scope, util) {

    $scope.passwordChangeDisabled = false;
    $scope.passwordChangeWaiting = false;
    $scope.passwordChangeSucceeded = false;
    $scope.passwordChangeFailed = false;

    $scope.changeUserPassword = function() {
      $scope.passwordChangeDisabled = true;
      $scope.passwordChangeWaiting = true;
      $scope.passwordChangeSucceeded = false;
      $scope.passwordChangeFailed = false;

      util
        .post('/ajax/changeUserPassword', {
          oldPassword: $scope.oldPassword,
          newPassword: $scope.newPassword,
          newPassword2: $scope.newPassword2,
        }).$status.then(function(success){
          $scope.passwordChangeWaiting = false;
          if (success) {
            $scope.passwordChangeSucceeded = true;
          } else {
            $scope.passwordChangeFailed = true;
            $scope.passwordChangeDisabled = false;
          }
        });
    };
  });
