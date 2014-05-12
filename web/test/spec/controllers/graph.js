'use strict';

describe('metagraph navigation', function () {
  beforeEach(module('biggraph'));

  var ctrl, scope, $httpBackend;

  beforeEach(inject(function ($injector, $controller, $rootScope) {
    scope = $rootScope.$new();
    // Mock $httpBackend.
    $httpBackend = $injector.get('$httpBackend');

    // $httpBackend.when('GET', '/ajax/graph?q={"id":"test"}').respond({
    $httpBackend.when('GET', '/ajax/graph?q=%7B%22id%22:%22test%22%7D').respond({
      'title': 'test node',
      'sources': [],
      'ops': [
        {'title': 'op 1', 'id': 'op1'},
        {'title': 'op 2', 'id': 'op2'},
      ],
    });
    // $httpBackend.when('GET', '/ajax/stats?q={"id":"test"}').respond({
    $httpBackend.when('GET', '/ajax/stats?q=%7B%22id%22:%22test%22%7D').respond({
      'id': 'test id',
      'vertices_count': '100',
      'edges_count': '1000',
    });
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
    expect(scope.graph.ops.length).toBe(2);
  });
});
