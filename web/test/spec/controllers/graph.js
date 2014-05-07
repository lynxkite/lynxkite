'use strict';

describe('metagraph navigation', function () {
  beforeEach(module('biggraph'));

  var ctrl, scope, $httpBackend;

  beforeEach(inject(function ($injector, $controller, $rootScope) {
    scope = $rootScope.$new();
    // Mock $httpBackend.
    $httpBackend = $injector.get('$httpBackend');
    $httpBackend.when('GET', '/ajax/graph/test').respond({
      'title': 'test node',
      'stats': 'test stats',
      'priors': [],
      'ops': [
        {'title': 'op 1', 'id': 'op1'},
        {'title': 'op 2', 'id': 'op2'},
      ],
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
