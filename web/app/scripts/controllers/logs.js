// The "/logs" page for accessing LynxKite logs.
'use strict';

angular.module('biggraph')
  .controller('LogsCtrl', function ($scope, util, $window) {
    $scope.logFiles = util.nocache('/getLogFiles');

    $scope.downloadLogFile = function(fileName) {
      // Fire off the download.
      $window.location =
        '/getLogFile?q=' + encodeURIComponent(JSON.stringify({'name': fileName}));
    };

    var scalarCache = {}; // Need to return the same object every time to avoid digest hell.
    $scope.asScalar = function(value) {
      if (scalarCache[value] === undefined) {
        scalarCache[value] = { value: {
          string: value !== undefined ? value.toString() : '',
          double: value,
        }};
      }
      return scalarCache[value];
    };
 });
