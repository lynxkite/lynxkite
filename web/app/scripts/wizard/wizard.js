// The wizard interface for workspaces.
'use strict';

angular.module('biggraph')
  .controller('WizardCtrl', function ($scope, $routeParams, util, WorkspaceWrapper) {
    $scope.routeParams = $routeParams;
    $scope.util = util;
    $scope.expanded = 0;
    $scope.workspace = new WorkspaceWrapper($routeParams.name, {});
    $scope.steps = [];

    $scope.workspace.loadWorkspace().then(() => {
      $scope.steps = JSON.parse($scope.workspace.getBox('anchor').instance.parameters.steps);
      for (let step of $scope.steps) {
        step.title = step.title || $scope.workspace.getBox(step.box).instance.operationId;
      }
    });
  });
