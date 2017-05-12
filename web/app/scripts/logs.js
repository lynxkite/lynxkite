// The "/logs" page for accessing LynxKite logs.
'use strict';

angular.module('biggraph')
  .controller('LogsCtrl', function ($scope, util) {
    $scope.logFiles = util.nocache('/getLogFiles');

    $scope.asScalar = function(value) {
      return util.asScalar(value);
    };
  });
