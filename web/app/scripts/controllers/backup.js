// The "/backup" page for the S3 backup of data and metadata.
'use strict';

angular.module('biggraph')
  .controller('BackupCtrl', function ($scope, util) {
    $scope.inProgress = 0;
    $scope.status = '';
    $scope.success = false;
    $scope.s3MetadataDir = '';

    $scope.backupSettings = util.nocache('/ajax/getBackupSettings');
    $scope.$watch('backupSettings.s3MetadataBucket', function (){
      $scope.s3MetadataDir =
      's3://' + $scope.backupSettings.s3MetadataBucket +
      '/' + $scope.backupSettings.metadataVersionTimestamp + '/';
    });

    $scope.backupToS3 = function() {
      $scope.inProgress = 1;
      util.post('/ajax/s3Backup', {
        s3MetadataDir: $scope.s3MetadataDir,
      }).finally(function() {
        $scope.inProgress = 0;
      });
      $scope.status = 'Data backup is completed.';
      $scope.success = true;
    };

    $scope.isDisabled = function() {
      if ($scope.backupSettings.dataDir === '') {return true;}
      //if ($scope.backupSettings.emphemeralDataDir === '' ) {return true;}
      if ($scope.backupSettings.s3MetadataBucket === '') {return true;}
      return $scope.success;
    };

  });
