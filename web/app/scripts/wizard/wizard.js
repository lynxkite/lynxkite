// The wizard interface for workspaces.
'use strict';

angular.module('biggraph')
  .controller('WizardCtrl', function ($scope, $routeParams, util, WorkspaceWrapper, $location, $window, $timeout) {
    const md = window.markdownit();
    const path = $routeParams.name.split('/');
    if (path.includes('In progress wizards')) { // These have a timestamp that we hide.
      $scope.name = path[path.length - 2];
    } else {
      $scope.name = path[path.length - 1];
    }
    util.scopeTitle($scope, $scope.name);
    $scope.util = util;
    $scope.expanded = 0;
    $scope.maximized = false;
    $scope.$window = $window;
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
      $location.url(`/dir/Users/${util.user.email}/In progress wizards`);
    };

    $scope.toggleMaximized = function() {
      $scope.maximized = !$scope.maximized;
    };

    $scope.moveToStep = function(i) {
      // Timeout to wait for delayed blur events to trigger workspace changes.
      // Then wait for the new state IDs.
      $timeout(() => $scope.workspace.loading.then(() => {
        $scope.expanded = i;
      }));
    };

    $scope.isShowingVisualization = function() {
      if (!$scope.steps) {
        return false;
      }
      const step = $scope.steps[$scope.expanded];
      if (!step || !step.popup || step.popup === 'parameters') {
        return false;
      }
      const p = $scope.workspace.getOutputPlug(step.box, step.popup);
      return p.kind === 'visualization';
    };
  });
