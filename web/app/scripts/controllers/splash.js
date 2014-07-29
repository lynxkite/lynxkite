'use strict';

angular.module('biggraph')
  .controller('SplashCtrl', function ($scope, $resource, $location) {
    $scope.data = $resource('/ajax/splash').get({ q: { fake: 1 } });
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
