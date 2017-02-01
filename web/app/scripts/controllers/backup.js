// The "/backup" page for the S3 backup of data and metadata.
'use strict';

angular.module('biggraph')
  .controller('BackupCtrl', function ($scope, util) {
    $scope.inProgress = false;
    $scope.statusMessage = '';
    $scope.success = false;
    $scope.backupSettings = util.nocache('/ajax/getBackupSettings');

    $scope.backupToS3 = function() {
      $scope.inProgress = true;
      $scope.statusMessage = '';
      util.post('/ajax/s3Backup', {
        timestamp: $scope.backupSettings.metadataVersionTimestamp,
      }).then(function() { // Success
        $scope.inProgress = false;
        $scope.statusMessage = 'Data backup is completed.';
        $scope.success = true;
      }, function(reason) { // failure
        $scope.inProgress = false;
        $scope.statusMessage = 'Data backup is failed. Error: ' + reason.statusText;
        $scope.success = false;
      });
    };

    $scope.isDisabled = function() {
      if ($scope.backupSettings.dataDir === '') {
        return true;
      }
      if ($scope.backupSettings.emphemeralDataDir === '' ) {
        return true;
      }
      if ($scope.inProgress) {
        return true;
      }
      return $scope.success;
    };

  });
