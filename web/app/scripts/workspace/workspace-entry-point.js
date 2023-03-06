// The '/workspace/WorkSpaceName' page.

'use strict';
import '../app';

angular.module('biggraph')
  .controller('WorkspaceEntryPointCtrl', function ($scope, $routeParams) {
    $scope.routeParams = $routeParams;
    $scope.boxCatalog = {};
  });
