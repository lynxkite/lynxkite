'use strict';

angular.module('biggraph')
  .controller('SplashCtrl', function ($scope, $resource, $location) {
    $scope.data = $resource('/ajax/splash').get();
    $scope.createProject = function() {
      $scope.newProject.sending = true;
      $resource('/ajax/createProject').save($scope.newProject, function() {
        $location.path('/project/' + $scope.newProject.name);
      }, function(error) {
        console.log(error);
        $scope.newProject.sending = false;
      });
    };
  });
