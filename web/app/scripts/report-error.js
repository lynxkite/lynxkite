// The modal dialog for error reporting.
'use strict';

angular.module('biggraph').controller('ReportErrorCtrl', function($scope, $uibModalInstance, alert, util) {
  /* global jsyaml */
  const debug = {
    message: alert.message,
    details: alert.details,
    url: window.location.href,
    version: util.globals.version,
  };
  $scope.debug = alert.details ? jsyaml.safeDump(debug, { sortKeys: true }) : undefined;
  $scope.title = alert.title || 'Reporting errors';

  $scope.close = function() {
    $uibModalInstance.dismiss('close');
  };

  /* global ClipboardJS */
  const clippy = new ClipboardJS('#copy-debug-to-clipboard');
  $scope.$on('$destroy', () => clippy.destroy());
});
