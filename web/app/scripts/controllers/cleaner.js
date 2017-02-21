// The "/cleaner" page for LynxKite cleanup utilities.
'use strict';

angular.module('biggraph')
  .controller('CleanerCtrl', function ($scope, util) {
    $scope.inProgress = 1;
    $scope.getDataFilesStatus = function() {
      $scope.fileStatus = util.nocache('/ajax/getDataFilesStatus');
      $scope.fileStatus.finally(function() {
        $scope.inProgress -= 1;
      });
    };
    $scope.getDataFilesStatus();

    $scope.moveToTrash = function(method) {
      $scope.inProgress += 1;
      util
        .post('/ajax/moveToCleanerTrash', {
          method: method,
        }).finally(function() {
          $scope.getDataFilesStatus();
        });
    };

    $scope.emptyTrash = function() {
      $scope.inProgress += 1;
      util
        .post('/ajax/emptyCleanerTrash', {
          fake: 0,
        }).finally(function() {
          $scope.getDataFilesStatus();
        });
    };

    $scope.asScalar = function(value) {
      return util.asScalar(value);
    };
  });
