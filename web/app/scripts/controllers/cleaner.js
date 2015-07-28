// The "/cleaner" page for LynxKite cleanup utilities.
'use strict';

angular.module('biggraph')
  .controller('CleanerCtrl', function ($scope, util) {
    $scope.fileStatus = util.nocache('/ajax/getDataFilesStatus');

    $scope.deleteOrphanFiles = function() {
      console.log($scope.selectedMethod);
      util
        .post('/ajax/markFilesDeleted', {
          method: $scope.selectedMethod,
        }).$status.then(function() {
          $scope.fileStatus = util.nocache('/ajax/getDataFilesStatus');
        });
    };
  });
