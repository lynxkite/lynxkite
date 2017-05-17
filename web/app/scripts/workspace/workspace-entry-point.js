// The '/workspace/WorkSpaceName' page.

'use strict';

angular.module('biggraph')
  .controller('WorkspaceEntryPointCtrl', function ($scope, $routeParams) {
    $scope.routeParams = $routeParams;
    $scope.boxCatalog = {};
  });
