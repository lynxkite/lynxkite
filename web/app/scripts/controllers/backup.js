// The "/backup" page for the S3 backup of data and metadata.
'use strict';

angular.module('biggraph')
  .controller('BackupCtrl', function ($scope, util) {
    $scope.inProgress = 1;
    $scope.status = '';
    $scope.success = false;

    $scope.backupSettings = util.nocache('/ajax/getBackupSettings');

    $scope.backupToS3 = function() {
      $scope.status = 'Data backup is completed.';
      $scope.success = true;
      $scope.inProgress = 0;
    };

    $scope.isDisabled = function() {
      if ($scope.backupSettings.dataDir === '' ||
          $scope.backupSettings.dataDir === 'UNDEF') {
         return true;
      }
      if ($scope.backupSettings.emphemeralDataDir === '' //||
          //$scope.backupSettings.emphemeralDataDir === 'UNDEF'
          ) {
         return true;
      }
      if ($scope.backupSettings.s3MetadataBucket === '') {return true;}
      return $scope.success;
    };

  });
