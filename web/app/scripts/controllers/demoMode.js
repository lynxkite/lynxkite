// The "/demoMode" page allows toggling the demo mode.
'use strict';

angular.module('biggraph')
  .controller('DemoModeCtrl', function ($scope, util) {
    $scope.status = util.nocache('/ajax/demoModeStatus', {fake: 0});

    function exitReq() {
      return util.nocache('/ajax/exitDemoMode', {fake: 0});
    }
    function enterReq() {
      return util.nocache('/ajax/enterDemoMode', {fake: 0});
    }
    $scope.switchMode = function() {
      var request = $scope.status.demoMode ? exitReq() : enterReq();
      request.then(function () {
        $scope.status = util.nocache('/ajax/demoModeStatus', {fake: 0});
      });
    };
  });
