// The "/backup" page for the S3 backup of data and metadata.
'use strict';

angular.module('biggraph')
  .controller('BackupCtrl', function ($scope, util) {
    $scope.inProgress = 0;
    $scope.status = '';
    $scope.success = false;
    $scope.backupSettings = util.nocache('/ajax/getBackupSettings');

    $scope.backupToS3 = function() {
      $scope.inProgress = 1;
      util.post('/ajax/s3Backup', {
        timestamp: $scope.backupSettings.metadataVersionTimestamp,
      }).finally(function() {
        $scope.inProgress = 0;
      });
      $scope.status = 'Data backup is completed.';
      $scope.success = true;
    };

    $scope.isDisabled = function() {
      if ($scope.backupSettings.dataDir === '') {return true;}
      //if ($scope.backupSettings.emphemeralDataDir === '' ) {return true;}
      return $scope.success;
    };

  });
