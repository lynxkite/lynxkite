// The "/" page displays branding and the project list.
import '../app';
import '../util/util';

angular.module('biggraph')
  .controller('SplashCtrl', ['$scope', 'util', function ($scope, util) {
    $scope.util = util;
  }]);
