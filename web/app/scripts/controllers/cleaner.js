// The "/cleaner" page for LynxKite cleanup utilities.
'use strict';

angular.module('biggraph')
  .controller('CleanerCtrl', function ($scope, util) {
    $scope.fileStatus = util.nocache('/ajax/getDataFilesStatus');

    $scope.markFilesDeleted = function() {
      util
        .post('/ajax/markFilesDeleted', {
          method: $scope.selectedMethod,
        }).finally(function() {
          $scope.fileStatus = util.nocache('/ajax/getDataFilesStatus');
        });
    };

    $scope.deleteMarkedFiles = function() {
      util
        .post('/ajax/deleteMarkedFiles', {
          fake: 0,
        }).finally(function() {
          $scope.fileStatus = util.nocache('/ajax/getDataFilesStatus');
        });
    };

    $scope.asScalar = function(value) {
      return util.asScalar(value);
    };
  });
