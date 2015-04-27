// The "/cluster" page allows resizing the cluster.
'use strict';

angular.module('biggraph')
  .controller('ClusterManagerCtrl', function ($scope, $resource) {
    var ClusterStatus = $resource('/ajax/sparkStatus');
    var SetWorkers = $resource('/ajax/setWorkers');

    $scope.status = ClusterStatus.get({q: {fake: 0}});

    $scope.setWorkers = function(numWorkers) {
      var setWorkersRequest = {
        workerInstances: numWorkers
      };
      $scope.status = SetWorkers.get({q: setWorkersRequest});
    };
  });
