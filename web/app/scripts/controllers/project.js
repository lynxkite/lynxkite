'use strict';

angular.module('biggraph')
  .controller('ProjectViewCtrl', function ($scope, $routeParams, $resource, $location) {
    function get(url, params) {
      var req = $resource(url).get(params, function() {}, function(failure) {
        req.error = 'Request failed: ' + failure.data;
      });
      return req;
    }
    $scope.left = {};
    $scope.right = {};
    $scope.left.data = get('/ajax/project', {project: $routeParams.project});

    var ISO = ['', 'k', 'M', 'G', 'T', 'P', 'E'];
    $scope.human = function(x) {
      for (var i = 0; i < ISO.length; ++i) {
        if (x < 1000) { return x + ISO[i]; }
        x = Math.round(x / 1000);
      }
      return x + ISO[i];
    };
  });
