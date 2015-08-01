// The "/changepassword" page 
'use strict';

angular.module('biggraph')
  .controller('ChangePasswordCtrl', function ($scope, util) {
    $scope.passwordChanged = false;
    $scope.changeUserPassword = function() {
      util
        .post('/ajax/changeUserPassword', {
          oldPassword: $scope.oldPassword,
          newPassword: $scope.newPassword,
          newPassword2: $scope.newPassword2,
        }).$status.then(function(success) {
            if (success) {
              $scope.passwordChanged = true;
            }
        });
    };
  });
