'use strict';

angular.module('biggraph')
  .controller('SplashCtrl', function ($scope, $resource, $location) {
    $scope.data = $resource('/ajax/splash').get();
  });
