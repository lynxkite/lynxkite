// The "/backup" page for the S3 backup of data and metadata.
'use strict';

angular.module('biggraph')
  .controller('BackupCtrl', function ($scope, util) {
    $scope.inProgress = 0;
    $scope.backupSettings = util.nocache('/ajax/getBackupSettings');

    $scope.doBackup = function() {
      $scope.status = 'Backup in progress';

    };


  });
