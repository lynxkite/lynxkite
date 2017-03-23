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

  $scope.boxCatalog = util.nocache('/ajax/boxCatalog');
  $scope.$watchGroup(
    ['boxCatalog.$resolved'],
    function() {
      if ($scope.boxCatalog.$resolved) {
        $scope.workspace = workspace(
            $scope.boxCatalog,
            $routeParams.workspaceName);
      }
  });
});


