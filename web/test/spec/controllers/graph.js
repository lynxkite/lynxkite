'use strict';

describe('metagraph navigation', function () {
  beforeEach(module('biggraph'));

  var ctrl, scope, $httpBackend;

  beforeEach(inject(function ($injector, $controller, $rootScope) {
    scope = $rootScope.$new();
    // Mock $httpBackend.
    $httpBackend = $injector.get('$httpBackend');
    var request = {id: 'test'};
    var requestJson = encodeURI(JSON.stringify(request));
    $httpBackend.when('GET', '/ajax/graph?q=' + requestJson).respond({
      'title': 'test node',
      'sources': [],
      'derivatives': [],
      'ops': [
        {
          'operationId': 0,
          'name': 'Find Maximal Cliques',
          'parameters': [{'title': 'Minimum Clique Size', 'defaultValue': '3'}],
        }
      ],
    });
    $httpBackend.when('GET', '/ajax/stats?q=' + requestJson).respond({
      'id': 'test',
      'verticesCount': '100',
      'edgesCount': '1000',
    });
    var emptyRequest = {fake: 0};
    var emptyRequestJson = encodeURI(JSON.stringify(emptyRequest));
    $httpBackend.when('GET', '/ajax/startingOps?q=' + emptyRequestJson).respond([]);
    ctrl = $controller('GraphViewCtrl', {
      $scope: scope,
      $routeParams: {graph: 'test'},
    });
  }));

  afterEach(function() {
    $httpBackend.verifyNoOutstandingExpectation();
    $httpBackend.verifyNoOutstandingRequest();
  });

  it('should make an HTTP request', function() {
    $httpBackend.flush();
    expect(scope.graph.ops.length).toBe(1);
    expect(scope.stats.verticesCount).toBe('100');
  });
});
