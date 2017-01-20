// The "/backup" page for the S3 backup of data and metadata.
'use strict';

angular.module('biggraph')
  .controller('BackupCtrl', function ($scope) {
    $scope.s3DataDir = 's3://data-bucket';
    $scope.s3MetadataDir = 's3://metadata-bucket';
    $scope.status = '';

    $scope.doBackup = function() {
      $scope.status = 'Backup in progress';

    };


  });
