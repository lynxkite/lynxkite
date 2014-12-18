'use strict';

var loginScope;
angular.module('biggraph')
  .controller('LoginCtrl', function ($scope, $location, util) {
    loginScope = $scope;
    $scope.login = function() {
      $scope.credentials = { username: '', password: '' };
      util.post('/login', $scope.credentials).then(function(success) {
        $scope.submitted = false;
        if (success) {
          $location.url('/');
        }
      });
      $scope.submitted = true;
    };
    $scope.googleSignIn = function(code) {
      console.log('code', code);
    };
  });

/* exported googleSignInCallback */
function googleSignInCallback(authResult) {
  loginScope.googleSignIn(authResult.code);
}
