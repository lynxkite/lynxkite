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

    var scalarCache = {}; // Need to return the same object every time to avoid digest hell.
    $scope.asScalar = function(value) {
      if (scalarCache[value] === undefined) {
        scalarCache[value] = { value: {
          string: value !== undefined ? value.toString() : '',
          double: value,
        }};
      }
      return scalarCache[value];
    };
  });
