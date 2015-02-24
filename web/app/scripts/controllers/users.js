'use strict';

angular.module('biggraph')
  .controller('UsersCtrl', function ($scope, util) {
    $scope.list = util.nocache('/ajax/getUsers');

    $scope.createUser = function() {
      util
        .post('/ajax/createUser', {
          email: $scope.newUserName,
          password: $scope.newPassword,
          isAdmin: $scope.newUserIsAdmin || false,
        }).then(function() {
          $scope.list = util.nocache('/ajax/getUsers');
        });
    };

    $scope.reportListError = function() {
      util.reportRequestError($scope.list, 'User list could not be loaded.');
    };
  });
