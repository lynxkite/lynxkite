'use strict';

angular.module('biggraph')
  .controller('GraphViewCtrl', function ($scope, $routeParams, $resource, $modal, $location) {
    function openOperationModal(operation) {
      var modalInstance = $modal.open({
        templateUrl: 'views/operationParameters.html',
        controller: 'OperationParametersCtrl',
        resolve: {
          operation: function() {
            return operation;
          }
        }
      });
      return modalInstance.result;
    }

    var DerivedGraph = $resource('/ajax/derive');
    function jumpToDerivedGraph(operation, modalResult, sourceIds) {
      var deriveRequest = {
        sourceIds: sourceIds,
        operation: {
          operationId: operation.operationId,
          parameters: modalResult
        }
      };
      DerivedGraph.get({q: deriveRequest}, function(derivedGraph) {
        $location.url('/graph/' + derivedGraph.id);
      });
    }

    function deriveGraphFlow(operation, sourceIds) {
      openOperationModal(operation).then(function(modalResult) {
        jumpToDerivedGraph(operation, modalResult, sourceIds);
      });
    }

    function openSaveToCSVModal() {
      var modalInstance = $modal.open({
        templateUrl: 'views/fileDialog.html',
        controller: 'FileDialogCtrl',
        resolve: {
          question: function() {
            return 'Enter directory name for saving current graph';
          }
        }
      });
      return modalInstance.result;
    }

    var SaveGraphAsCSV = $resource('/ajax/saveAsCSV');
    function sendSaveToCSVRequest(id, path) {
      var saveRequest = {
        id: id,
        targetDirPath: path
      };
      SaveGraphAsCSV.get({q: saveRequest}, function(response) {
        // TODO: report in the status bar instead once we have one.
        if (response.success) {
          window.alert('Graph saved successfully');
        } else {
          window.alert('Graph saving failed: ' + response.failureReason);
        }
      });
    }

    function saveCSVFlow(id) {
      openSaveToCSVModal().then(function(path) {
        sendSaveToCSVRequest(id, path);
      });
    }

    var StartingOps = $resource('/ajax/startingOps');
    $scope.startingOps = StartingOps.query({q: {fake: 0}});

    $scope.openNewGraphModal = function(operation) {
      deriveGraphFlow(operation, []);
    };

    var id = $routeParams.graph;
    if (id !== 'x') {
      var Graph = $resource('/ajax/graph');
      var Stats = $resource('/ajax/stats');

      $scope.id = id;
      $scope.graph = Graph.get({q: {id: id}});
      $scope.stats = Stats.get({q: {id: id}});

      $scope.graphView = $resource('/ajax/bucketed').get({q: {axisX: 'age', axisY: 'income', filters: []}});
 
      $scope.saveCSV = function() {
        saveCSVFlow(id);
      };

      $scope.openDerivationModal = function(operation) {
        deriveGraphFlow(operation, [id]);
      };
    }
  });

angular.module('biggraph')
  .directive('graphView', function() {
    return {
      template: '<svg class="graph-view" version="1.1" xmlns="http://www.w3.org/2000/svg"><defs></defs><g class="root"></g></svg>',
      require: '^ngModel',
      replace: true,
      link: function(scope, element, attrs) {
        var svg = angular.element(element);
        var defs = svg.find('defs');
        var root = svg.find('g.root');
        var grad = SVG.gradient(SVG.hsl(0, 50, 70), SVG.hsl(0, 50, 10));
        defs.append(grad);

        var edges = SVG.create('g', {'class': 'edge'});
        var nodes = SVG.create('g', {'class': 'node'});
        root.append([edges, nodes]);

        function Node(x, y, r) {
          var c = SVG.create('circle', {cx: x, cy: y, r: r, fill: grad.color});
          var highlight = SVG.create('circle', {cx: x - 0.4 * r, cy: y - 0.4 * r, r: r * 0.1, fill: 'white'});
          var g = SVG.group([c, highlight]);
          console.log(nodes);
          nodes.append(g);
          return g;
        }

        function Edge(x1, y1, x2, y2, w) {
          var a = SVG.arrow(x1, y1, x2, y2, w);
          edges.append(a);
          return a;
        }

        scope.$watch(attrs.ngModel, function(graph) {
          if (!graph.$resolved) { return; }
          nodes.empty();
          edges.empty();
          var nodeScale = 0.01 / SVG.minmax(graph.nodes.map(function(n) { return n.count; })).max;
          var xb = SVG.minmax(graph.nodes.map(function(n) { return n.x; }));
          var yb = SVG.minmax(graph.nodes.map(function(n) { return n.y; }));
          for (var i = 0; i < graph.nodes.length; ++i) {
            var node = graph.nodes[i];
            var n = Node(SVG.normalize(node.x, xb), SVG.normalize(node.y, yb),
                         Math.sqrt(nodeScale * node.count));
          }
          var edgeScale = 0.005 / SVG.minmax(graph.nodes.map(function(n) { return n.count; })).max;
          for (var i = 0; i < graph.edges.length; ++i) {
            var edge = graph.edges[i];
            var a = graph.nodes[edge.a];
            var b = graph.nodes[edge.b];
            var e = Edge(SVG.normalize(a.x, xb), SVG.normalize(a.y, yb),
                         SVG.normalize(b.x, xb), SVG.normalize(b.y, yb),
                         edgeScale * edge.count);
          }
        }, true);
      },
    };
  });
