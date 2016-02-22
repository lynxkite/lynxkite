// The "/logs" page for accessing LynxKite logs.
'use strict';

angular.module('biggraph')
  .controller('LogsCtrl', function ($scope, util, $window) {
    $scope.logFiles = util.nocache('/getLogFiles');

    $scope.downloadLogFile = function(fileName) {
      // Fire off the download.
      $window.location =
        '/downloadLogFile?q=' + encodeURIComponent(JSON.stringify({'name': fileName}));
    };

    $scope.asScalar = function(value) {
      return util.asScalar(value);
    };
  });
