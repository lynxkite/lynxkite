// The modal dialog for error reporting.
'use strict';
import './app';
import './util/util';
import jsyaml from 'js-yaml';
import ClipboardJS from 'clipboard';

angular.module('biggraph').controller('ReportErrorCtrl', ["$scope", "$uibModalInstance", "alert", "util", function($scope, $uibModalInstance, alert, util) {
  const debug = {
    message: alert.message,
    details: alert.details,
    url: window.location.href,
    version: util.globals.version,
  };
  $scope.debug = alert.details ? jsyaml.dump(debug, { sortKeys: true, skipInvalid: true }) : undefined;
  $scope.title = alert.title || 'Reporting errors';

  $scope.close = function() {
    $uibModalInstance.dismiss('close');
  };

  const clippy = new ClipboardJS('#copy-debug-to-clipboard');
  $scope.$on('$destroy', () => clippy.destroy());
}]);
