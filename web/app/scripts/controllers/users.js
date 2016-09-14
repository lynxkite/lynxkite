// The "/users" page for managing LynxKite users.
'use strict';

angular.module('biggraph')
  .controller('UsersCtrl', function ($scope, util) {
    $scope.list = util.nocache('/ajax/getUsers');
    $scope.password = {};
    $scope.userIsAdmin = {};

    $scope.createUser = function() {
      util
        .post('/ajax/createUser', {
          email: $scope.newUserName,
          password: $scope.newPassword,
          isAdmin: $scope.newUserIsAdmin || false,
        }).$status.then(function() {
          $scope.list = util.nocache('/ajax/getUsers');
        });
    };

    $scope.changeUser = function(email) {
      console.log($scope.userIsAdmin);
      util
        .post('/ajax/changeUser', {
          email: email,
          password: $scope.password[email],
          isAdmin: $scope.userIsAdmin[email],
        }).$status.then(function() {
          $scope.password = {};
          $scope.list = util.nocache('/ajax/getUsers');
        });
    };

    $scope.reportListError = function() {
      util.reportRequestError($scope.list, 'User list could not be loaded.');
    };
  });
