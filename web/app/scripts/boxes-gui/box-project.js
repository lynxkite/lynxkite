// The '/boxproject' page.
'use strict';

angular.module('biggraph')
  .controller('BoxProjectCtrl', function ($scope, $routeParams, util) {

  $scope.workspaceName = $routeParams.workspaceName;

  $scope.diagram = util.nocache(
    '/ajax/getWorkspace',
    {
      name: $scope.workspaceName
    });

  $scope.allBoxes = util.nocache('/ajax/boxCatalog');

});


