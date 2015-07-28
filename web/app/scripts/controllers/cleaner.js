// The "/cleaner" page for LynxKite cleanup utilities.
'use strict';

angular.module('biggraph')
  .controller('CleanerCtrl', function ($scope, util) {
    $scope.methods = util.nocache('/ajax/getCleaner');

    $scope.deleteOrphanFiles = function() {
      util
        .post('/ajax/markFilesDeleted', {
          method: $scope.selectedMethod,
        }).$status.then(function() {
          $scope.methods = util.nocache('/ajax/getCleaner');
        });
    };
  });
