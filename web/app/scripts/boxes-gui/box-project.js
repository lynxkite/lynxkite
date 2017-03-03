// The '/boxproject' page.
'use strict';

angular.module('biggraph')
  .controller('BoxProjectCtrl', function ($scope, $routeParams, util) {

  $scope.workspaceName = $routeParams.workspaceName;

  $scope.allBoxes = util.nocache('/ajax/boxCatalog');

});


