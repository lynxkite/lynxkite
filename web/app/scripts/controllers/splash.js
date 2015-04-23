// The "/" page displays branding and the project list.
'use strict';

angular.module('biggraph')
  .controller('SplashCtrl', function ($scope, $location) {
    $scope.$watch('name', function(name) {
      if (name !== undefined) {
        $location.url('/project/' + name);
      }
    });
  });
