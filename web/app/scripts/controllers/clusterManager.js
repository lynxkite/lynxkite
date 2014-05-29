'use strict';

angular.module('biggraph')
  .controller('ClusterManagerCtrl', function ($scope, $routeParams, $resource) {
    var password = $routeParams.password;

    var ClusterStatus = $resource('/ajax/sparkStatus');
    var SetWorkers = $resource('/ajax/setWorkers');

    $scope.status = ClusterStatus.get({q: {fake: 0}});

    $scope.setWorkers = function(numWorkers) {
      var setWorkersRequest = {
        password: password,
        workerInstances: numWorkers
      };
      $scope.status = SetWorkers.get({q: setWorkersRequest});
    };
  });
