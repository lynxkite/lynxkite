// The "/login" page for logging in.
'use strict';

let loginScope;
angular.module('biggraph')
  .controller('LoginCtrl', function ($scope, $location, util) {
    $scope.util = util;
    loginScope = $scope;
    $scope.credentials = { username: '', password: '', method: 'lynxkite' };
    $scope.originalPath =
      'originalPath' in $location.search() ? $location.search()['originalPath'] : '/';
    $scope.$watch('util.globals', function() {
      util.globals.then(function() {
        $scope.passwordSignIn = !util.globals.googleClientId;
        $scope.googleClientId = util.globals.googleClientId;
      });
    });

    function tryLogin(url, credentials) {
      util.post(url, credentials).$status.then(function(success) {
        $scope.submitted = false;
        if (success) {
          util.clearAlerts();
          $location.url($scope.originalPath);
          util.reloadUser();
        }
      });
      $scope.submitted = true;
    }

    $scope.passwordLogin = function() {
      tryLogin('/passwordLogin', $scope.credentials);
    };

    $scope.googleLogin = function(googleUser) {
      /* eslint-disable camelcase, no-console */
      if (!googleUser) { return; }
      const id_token = googleUser.getAuthResponse().id_token;
      console.log(
        'Google login successful!' +
          ' If you want to use this login in a notebook environment, please set:');
      console.log('LYNXKITE_OAUTH_TOKEN=' + id_token);
      tryLogin('/googleLogin', { id_token: id_token });
    };

    $scope.showAuthMethods = function() {
      return util.globals.authMethods && util.globals.authMethods.length > 1;
    };
  });

/* eslint-disable no-unused-vars */
function googleSignInCallback(googleUser) {
  loginScope.googleLogin(googleUser);
}
