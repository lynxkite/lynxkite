// The '/workspace/WorkSpaceName' page.
'use strict';

angular.module('biggraph')
  .controller('WorkspaceMainCtrl', function ($scope, $routeParams, util, workspaceManager) {

  $scope.workspaceName = $routeParams.workspaceName;
  $scope.boxCatalog = util.nocache('/ajax/boxCatalog');
  $scope.$watchGroup(
    ['boxCatalog.$resolved', 'workspaceName'],
    function() {
      if ($scope.boxCatalog.$resolved && $scope.workspaceName) {
        $scope.manager = workspaceManager(
            $scope.boxCatalog, $scope.workspaceName);
      }
  });
});


