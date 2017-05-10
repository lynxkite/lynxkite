// The '/workspace/WorkSpaceName' page.

'use strict';

angular.module('biggraph')
  .controller('WorkspaceEntryPointCtrl', function ($scope, $routeParams, util) {
    $scope.routeParams = $routeParams;
    $scope.boxCatalog = util.nocache('/ajax/boxCatalog');
  });
