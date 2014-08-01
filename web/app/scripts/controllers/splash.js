'use strict';

angular.module('biggraph')
  .controller('SplashCtrl', function ($scope, $resource, $location, util) {
    $scope.data = util.nocache('/ajax/splash');
    $scope.createProject = function() {
      $scope.newProject.sending = true;
      var name = $scope.newProject.name.replace(/ /g, '_');
      var notes = $scope.newProject.notes;
      $resource('/ajax/createProject').save({ name: name, notes: notes }, function() {
        $location.path('/project/' + name);
      }, function(error) {
        console.log(error);
        $scope.newProject.sending = false;
      });
    };
  });
