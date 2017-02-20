// The "/login" page for logging in.
'use strict';

var loginScope;
angular.module('biggraph')
  .controller('LoginCtrl', function ($scope, $location, util) {
    $scope.util = util;
    loginScope = $scope;
    $scope.credentials = { username: '', password: '', method: 'lynxkite' };
    // OAuth is only set up on pizzakite.lynxanalytics.com.
    $scope.passwordSignIn = window.location.hostname !== 'pizzakite.lynxanalytics.com';

    function tryLogin(url, credentials) {
      util.post(url, credentials).$status.then(function(success) {
        $scope.submitted = false;
        if (success) {
          $location.url('/');
          util.reloadUser();
        }
      });
      $scope.submitted = true;
    }

    $scope.passwordLogin = function() {
      tryLogin('/passwordLogin', $scope.credentials);
    };

    $scope.googleLogin = function(googleUser) {
      /* jshint camelcase: false */
      if (!googleUser) { return; }
      var id_token = googleUser.getAuthResponse().id_token;
      console.log(
          'Google login successful!' +
          ' If you want to use this login in a notebook environment, please set:');
      console.log('LYNXKITE_OAUTH_TOKEN=' + id_token);
      tryLogin('/googleLogin', { id_token: id_token });
    };

    $scope.showAuthMethods = function() {
      return util.globals.authMethods.length > 1;
    };
  });

/* exported googleSignInCallback */
function googleSignInCallback(googleUser) {
  loginScope.googleLogin(googleUser);
}
