// The "/cleaner" page for LynxKite cleanup utilities.
'use strict';

angular.module('biggraph')
  .controller('CleanerCtrl', function ($scope, util) {
    $scope.fileStatus = util.nocache('/ajax/getDataFilesStatus');

    $scope.markFilesDeleted = function() {
      util
        .post('/ajax/markFilesDeleted', {
          method: $scope.selectedMethod,
        }).$status.then(function() {
          $scope.fileStatus = util.nocache('/ajax/getDataFilesStatus');
        });
    };

    $scope.deleteOrphanFiles = function() {
      util
        .post('/ajax/deleteOrphanFiles', {
          fake: 0,
        }).$status.then(function() {
          $scope.fileStatus = util.nocache('/ajax/getDataFilesStatus');
        });
    };
  });
