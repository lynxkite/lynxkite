// The '/workspace/WorkSpaceName' page.
'use strict';

angular.module('biggraph')
  .controller('WorkspaceMainCtrl', function ($scope, $routeParams, util) {

  $scope.workspaceName = $routeParams.workspaceName;

  $scope.boxCatalog = util.nocache('/ajax/boxCatalog');

});


