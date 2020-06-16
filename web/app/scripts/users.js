// The "/users" page for managing LynxKite users.
'use strict';

angular.module('biggraph')
  .controller('UsersCtrl', function ($scope, util) {
    $scope.list = util.nocache('/ajax/getUsers');
    $scope.password = {};
    $scope.userIsAdmin = {};
    $scope.userWizardOnly = {};

    $scope.createUser = function() {
      util
        .post('/ajax/createUser', {
          email: $scope.newUserName,
          password: $scope.newPassword,
          isAdmin: $scope.newUserIsAdmin || false,
          wizardOnly: $scope.newUserWizardOnly || false,
        }).$status.then(function() {
          $scope.list = util.nocache('/ajax/getUsers');
        });
    };

    $scope.changeUser = function(email) {
      util.warning('Are you sure?', 'User ' + email + ' will be changed.').then(() =>
        util
          .post('/ajax/changeUser', {
            email: email,
            password: $scope.password[email],
            isAdmin: $scope.userIsAdmin[email],
            wizardOnly: $scope.userWizardOnly[email],
          }).$status.then(function() {
            $scope.password = {};
            $scope.list = util.nocache('/ajax/getUsers');
          }));
    };

    $scope.deleteUser = function(email) {
      util.warning('Are you sure?', 'User ' + email + ' will be deleted.').then(() =>
        util
          .post('/ajax/deleteUser', {
            email: email,
          }).$status.then(function() {
            $scope.list = util.nocache('/ajax/getUsers');
          }));
    };
  });
