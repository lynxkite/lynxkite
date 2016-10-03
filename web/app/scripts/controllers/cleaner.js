// The "/cleaner" page for LynxKite cleanup utilities.
'use strict';

angular.module('biggraph')
  .controller('CleanerCtrl', function ($scope, util) {
    $scope.inProgress = 0;
    $scope.fileStatus = util.nocache('/ajax/getDataFilesStatus');

    $scope.markFilesDeleted = function() {
      $scope.inProgress += 1;
      util
        .post('/ajax/markFilesDeleted', {
          method: $scope.selectedMethod,
        }).finally(function() {
          $scope.fileStatus = util.nocache('/ajax/getDataFilesStatus');
          $scope.fileStatus.finally(function() {
            $scope.inProgress -= 1;
          });
        });
    };

    $scope.deleteMarkedFiles = function() {
      $scope.inProgress += 1;
      util
        .post('/ajax/deleteMarkedFiles', {
          fake: 0,
        }).finally(function() {
          $scope.fileStatus = util.nocache('/ajax/getDataFilesStatus');
          $scope.fileStatus.finally(function() {
            $scope.inProgress -= 1;
          });
        });
    };

    $scope.asScalar = function(value) {
      return util.asScalar(value);
    };
  });
