// The wizard interface for workspaces.
'use strict';

angular.module('biggraph')
  .controller('WizardCtrl', function ($scope, $routeParams, util, WorkspaceWrapper, $location) {
    const md = window.markdownit();
    const path = $routeParams.name.split('/');
    if (path.includes('In progress wizards')) { // These have a timestamp that we hide.
      $scope.name = path[path.length - 2];
    } else {
      $scope.name = path[path.length - 1];
    }
    $scope.util = util;
    $scope.expanded = 0;
    util.post('/ajax/openWizard', { name: $routeParams.name }).then(res => {
      if (res.name !== $routeParams.name) {
        $location.url('/wizard/' + res.name);
        $location.replace(); // So pressing "Back" will not make another copy.
        return;
      }
      $scope.steps = [];
      $scope.workspace = new WorkspaceWrapper(res.name, {});
      $scope.workspace.loadWorkspace().then(() => {
        $scope.steps = JSON.parse($scope.workspace.getBox('anchor').instance.parameters.steps);
        for (let step of $scope.steps) {
          step.title = step.title || $scope.workspace.getBox(step.box).instance.operationId;
          step.html = md.render(step.description || '');
        }
      });
    });

    $scope.goToWizardsInProgress = function() {
      window.sessionStorage.setItem(
        'last_selector_path', `Users/${util.user.email}/In progress wizards`);
      $location.url('/');
    };
  });
