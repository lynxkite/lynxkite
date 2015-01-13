'use strict';

angular.module('biggraph').controller('ReportErrorCtrl', function($scope, $modalInstance, alert) {
  $scope.message = alert.message || '';
  $scope.details = alert.details ? JSON.stringify(alert.details, null, '  ') : undefined;

  var support = 'pizza-support@lynxanalytics.com';
  var time = alert.time || Date();
  var body = 'Happened at ' + window.location.href + ' on ' + time;
  body += '\n\nPlease advise.';
  body += '\n\nError message:';
  body += '\n\n' + $scope.message;
  if ($scope.details) {
    body += '\n\nExtra info:';
    body += '\n\n' + $scope.details;
  }

  $scope.mailto = (
    'mailto:' + support +
    '?subject=' + encodeURIComponent('[Issue] ' + $scope.message.substr(0, 60)) +
    '&body=' + encodeURIComponent(body)
    );

  $scope.close = function() {
    $modalInstance.dismiss('close');
  };
});
