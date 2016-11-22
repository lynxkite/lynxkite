// The modal dialog for error reporting.
'use strict';

angular.module('biggraph').controller('ReportErrorCtrl', function($scope, $modalInstance, alert) {
  $scope.message = alert.message || '';
  $scope.details = alert.details ? JSON.stringify(alert.details, null, '  ') : undefined;

  var support = 'x+170265586669389@mail.asana.com'; // "Support requests" project in Asana.
  var time = alert.time || (new Date().toString());
  var body = $scope.message;
  if ($scope.details) {
    body += '\n\nExtra info:';
    body += '\n\n' + $scope.details;
  }
  body += '\n\nHappened at ' + window.location.href + ' on ' + time + '. Please advise.';
  // Limit body length to 800 characters to avoid hitting mailto limitations.
  body = body.slice(0, 800);

  $scope.mailto = (
    'mailto:' + support +
    '?subject=' + encodeURIComponent('⚠ ' + $scope.message.substr(0, 60)) +
    '&body=' + encodeURIComponent(body)
    );

  $scope.close = function() {
    $modalInstance.dismiss('close');
  };
});
