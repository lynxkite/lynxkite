'use strict';

angular.module('biggraph')
  .controller('SplashCtrl', function ($scope, $location) {
    $scope.$watch('name', function(name) {
      if (name !== undefined) {
        $location.path('/project/' + name);
      }
    });
  });
