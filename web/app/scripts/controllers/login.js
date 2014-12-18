'use strict';

var loginScope;
angular.module('biggraph')
  .controller('LoginCtrl', function ($scope, $location, util) {
    loginScope = $scope;
    $scope.credentials = { username: '', password: '' };
    // OAuth is only set up on pizzakite.lynxanalytics.com.
    $scope.passwordSignIn = window.location.hostname !== 'pizzakite.lynxanalytics.com';

    $scope.login = function() {
      util.post('/login', $scope.credentials).then(function(success) {
        $scope.submitted = false;
        if (success) {
          $location.url('/');
        }
      });
      $scope.submitted = true;
    };

    $scope.googleSignIn = function(code) {
      if (!code) { return; }
      util.post('/google', { code: code }).then(function(success) {
        $scope.submitted = false;
        if (success) {
          $location.url('/');
        }
      });
      $scope.submitted = true;
    };
  });

/* exported googleSignInCallback */
function googleSignInCallback(authResult) {
  loginScope.googleSignIn(authResult.code);
}
