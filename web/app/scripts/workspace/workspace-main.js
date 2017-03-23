// The '/workspace/WorkSpaceName' page.
//
// The loaded workspace is stored in a workspaceState that is
// wrapped in a workspaceManager. The state handles lower-level
// data structure issues, the manager handles transient GUI
// state, interaction with GUI and the backend.
//
// The components on this page (workspace-drawing-board, box-editor
// state-view) are all connected to the manager.
'use strict';

angular.module('biggraph')
  .controller('WorkspaceMainCtrl', function ($scope, $routeParams, util, workspace) {

  $scope.workspaceName = $routeParams.workspaceName;
  $scope.boxCatalog = util.nocache('/ajax/boxCatalog');
  $scope.$watchGroup(
    ['boxCatalog.$resolved', 'workspaceName'],
    function() {
      if ($scope.boxCatalog.$resolved && $scope.workspaceName) {
        $scope.manager = workspace($scope.boxCatalog, $scope.workspaceName);
      }
  });
});


