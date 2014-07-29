'use strict';

angular.module('biggraph')
  .controller('SplashCtrl', function ($scope, $resource, $location, nocache) {
    $scope.data = nocache('/ajax/splash');
    $scope.createProject = function() {
      $scope.newProject.sending = true;
      var id = $scope.newProject.name.replace(/ /g, '_');
      var notes = $scope.newProject.notes;
      $resource('/ajax/createProject').save({ id: id, notes: notes }, function() {
        $location.path('/project/' + id);
      }, function(error) {
        console.log(error);
        $scope.newProject.sending = false;
      });
    };
  });
