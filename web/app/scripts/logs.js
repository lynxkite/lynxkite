// The "/logs" page for accessing LynxKite logs.
'use strict';
import './app';
import './util/util';

angular.module('biggraph')
  .controller('LogsCtrl', ['$scope', 'util', function ($scope, util) {
    $scope.logFiles = util.nocache('/getLogFiles');

    $scope.asScalar = function(value) {
      return util.asScalar(value);
    };
  }]);
