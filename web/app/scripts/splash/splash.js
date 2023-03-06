// The "/" page displays branding and the project list.
'use strict';
import '../app';
import '../util/util';

angular.module('biggraph')
  .controller('SplashCtrl', function ($scope, util) {
    $scope.util = util;
  });
