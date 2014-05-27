'use strict';

angular.module('biggraph')
  .controller('ClusterManagerCtrl', function ($scope, $routeParams, $resource) {
    var password = $routeParams.password;

    var ClusterStatus = $resource('/ajax/sparkStatus?q=:request');
    var SetWorkers = $resource('/ajax/setWorkers?q=:request');

    var emptyRequest = {fake: 0};
    var emptyRequestJson = JSON.stringify(emptyRequest);
    $scope.status = ClusterStatus.get({request: emptyRequestJson});

    $scope.setWorkers = function(numWorkers) {
      var setWorkersRequest = {
        password: password,
        workerInstances: numWorkers
      };
      var setWorkersRequestJson = JSON.stringify(setWorkersRequest);
      $scope.status = SetWorkers.get({request: setWorkersRequestJson});
    };
  });
