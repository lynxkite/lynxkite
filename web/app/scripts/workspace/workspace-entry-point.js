// The '/workspace/WorkSpaceName' page.

'use strict';
import '../app';

angular.module('biggraph')
  .controller('WorkspaceEntryPointCtrl', ["$scope", "$routeParams", function ($scope, $routeParams) {
    $scope.routeParams = $routeParams;
    $scope.boxCatalog = {};
  }]);
