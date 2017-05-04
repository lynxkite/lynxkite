// The '/workspace/WorkSpaceName' page.

'use strict';

angular.module('biggraph')
  .controller('WorkspaceEntryPointCtrl', function ($scope, $routeParams, util, workspaceGuiMaster) {


    // Do not cache box catalog, so that custom boxes are up to date.
    $scope.boxCatalog = util.nocache('/ajax/boxCatalog');
    $scope.$watch(
      'boxCatalog.$resolved',
      function(resolved) {
        if (resolved) {
          $scope.guiMaster = workspaceGuiMaster(
            $scope.boxCatalog,
            $routeParams.workspaceName);
        }
      });
  });
