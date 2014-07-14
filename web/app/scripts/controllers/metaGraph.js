'use strict';

angular.module('biggraph')
  .controller('MetaGraphViewCtrl', function ($scope, $resource, $modal, $location) {
    $scope.alerts = [];
    $scope.closeAlert = function(index) { $scope.alerts.splice(index, 1); };
    $scope.deepWatch = function(expression, fn) { $scope.$watch(expression, fn, true); };

    function defaultSideState() {
      return {
        vs: undefined,
        filters: {},
        graphMode: undefined,
        bucketCount: 4,
        sampleRadius: 1,
        center: undefined,
      };
    }
    function defaultState() {
      return {
        leftToRightPath: undefined,
        left: defaultSideState(),
        right: defaultSideState(),
      };
    }
    $scope.state = defaultState();

    $scope.left = {};
    $scope.right = {};

    $scope.left.other = $scope.right;
    $scope.right.other = $scope.left;

    $scope.deepWatch(
      function() { return $location.search(); },
      function() {
        if (!$location.search().q) {
          $scope.state = defaultState();
        } else {
          var state = JSON.parse($location.search().q);
          if (!angular.equals(state, $scope.state)) {
            // The parts of the template that depend on 'state' get re-rendered
            // when we replace it. So we only do this if there is an actual
            // difference.
            $scope.state = state;
          }
        }
      });
    
    $scope.deepWatch(
      'state',
      function() {
        var s = $location.search();
        s.q = JSON.stringify($scope.state);
        $location.search(s);
      });

    var VertexSet = $resource('/ajax/vertexSet');
    function loadVertexSet(id) {
      var req = VertexSet.get({q: {id: id}}, function() {}, function(failure) {
        req.error = 'Request failed: ' + failure.data;
      });
      return req;
    }

    $scope.$watch(
      'state.left.vs',
      function() {
        var leftVS = $scope.state.left.vs;
        $scope.left.vertexSet = leftVS;
        if (leftVS !== undefined) {
          $scope.left.data = loadVertexSet(leftVS.id);
        } else {
          $scope.left.data = undefined;
        }
      });
    $scope.$watch(
      'state.right.vs',
      function() {
        var rightVS = $scope.state.right.vs;
        $scope.right.vertexSet = rightVS;
        if (rightVS !== undefined) {
          $scope.right.data = loadVertexSet(rightVS.id);
        } else {
          $scope.right.data = undefined;
        }
      });

    $scope.deepWatch('state', loadGraphView);
    function loadGraphView() {
      if (!$scope.showGraph()) { return; }
      var sides = [];
      if ($scope.state.left.graphMode !== undefined) { sides.push($scope.state.left); }
      if ($scope.state.right.graphMode !== undefined) { sides.push($scope.state.right); }
      if (sides.length === 0) { return; }
      var q = { vertexSets: [], edgeBundles: [] };
      for (var i = 0; i < sides.length; ++i) {
        var side = sides[i];
        if (side.edgeBundle !== undefined) {
          q.edgeBundles.push({
            srcDiagramId: 'idx[' + i + ']',
            dstDiagramId: 'idx[' + i + ']',
            srcIdx: i,
            dstIdx: i,
            bundleSequence: [{ bundle: side.edgeBundle.id, reversed: false }]
          });
        }
        var filters = [];
        for (var attr in side.filters) {
          if (side.filters[attr] !== '') {
            filters.push({ attributeId: attr, valueSpec: side.filters[attr] });
          }
        }
        q.vertexSets.push({
          vertexSetId: side.vs.id,
          filters: filters,
          mode: side.graphMode,
          // Bucketed view parameters.
          xBucketingAttributeId: side.xAttribute || '',
          yBucketingAttributeId: side.yAttribute || '',
          xNumBuckets: parseInt(side.bucketCount),  // angular.js/pull/7370
          yNumBuckets: parseInt(side.bucketCount),  // angular.js/pull/7370
          // Sampled view parameters.
          radius: parseInt(side.sampleRadius),  // angular.js/pull/7370
          centralVertexId: (side.center || '').toString(),
          sampleSmearEdgeBundleId: (side.edgeBundle || { id: '' }).id,
          labelAttributeId: side.labelAttribute || '',
          sizeAttributeId: side.sizeAttribute || '',
        });
      }
      if (sides.length === 2 && $scope.state.leftToRightPath !== undefined) {
        var bundles = $scope.state.leftToRightPath.map(function(step) {
          return { bundle: step.bundle.id, reversed: step.pointsLeft };
        });
        q.edgeBundles.push({
          srcDiagramId: 'idx[0]',
          dstDiagramId: 'idx[1]',
          srcIdx: 0,
          dstIdx: 1,
          bundleSequence: bundles,
        });
      }
      $scope.graphView = $resource('/ajax/complexView').get({ q: q });
    }
    $scope.showGraph = function() {
      return $scope.state.left.graphMode || $scope.state.right.graphMode;
    };

    $scope.deepWatch(
      'state', // TODO: Finer grained triggering.
      function() {
        if ($scope.left.data !== undefined) {
          $scope.left.data.$promise.then(function() { loadHistograms($scope.left); });
        }
        if ($scope.right.data !== undefined) {
          $scope.right.data.$promise.then(function() { loadHistograms($scope.right); });
        }
      });
    function loadHistogram(side, attr) {
      var filters = [];
      var state = side.state();
      var data = side.data;
      for (var filteredAttr in state.filters) {
        if (state.filters[filteredAttr] !== '') {
          filters.push({ attributeId: filteredAttr, valueSpec: state.filters[filteredAttr] });
        }
      }
      var q = {
        vertexSetId: data.id,
        filters: filters,
        mode: 'bucketed',
        xBucketingAttributeId: attr.id,
        xNumBuckets: 20,
        yBucketingAttributeId: '',
        yNumBuckets: 1,
        // Unused.
        centralVertexId: '',
        sampleSmearEdgeBundleId: '',
        radius: 0,
        labelAttributeId: '',
        sizeAttributeId: '',
      };
      attr.histogram = $resource('/ajax/vertexDiag').get({q: q});
    }
    function loadHistograms(side) {
      var data = side.data;
      for (var i = 0; i < data.attributes.length; ++i) {
        var a = data.attributes[i];
        if (a.showHistogram) {
          loadHistogram(side, a);
        }
      }
    }
    $scope.startToShowHistogram = function(side, attr) {
      attr.showHistogram = true;
      loadHistogram(side, attr);
    };

    var StartingVertexSets = $resource('/ajax/startingVs');
    function loadStartingVertexSets() {
      $scope.startingVertexSets = StartingVertexSets.query({q: {fake: 0}});
    }

    function openOperationModal(operation) {
      var modalInstance = $modal.open({
        templateUrl: 'views/operationParameters.html',
        controller: 'OperationParametersCtrl',
        backdrop: 'static',  // Do not close on backdrop click.
        resolve: {
          operation: function() {
            return operation;
          }
        }
      });
      return modalInstance.result;
    }

    var ApplyOperation = $resource('/ajax/applyOp');
    function applyOperation(operation, modalResult) {
      var request = {
        id: operation.id,
        parameters: modalResult
      };
      ApplyOperation.get(
        {q: request},
        function(result) {
          if (!result.success) {
            $scope.alerts.push({type: 'danger', msg: result.failureReason});
          }
          update();
        },
        function(response) {
          $scope.alerts.push({type: 'danger', msg: 'Request failed: ' + response.status});
          update();
        });
      function update() {
        // Force reloads of graphs by creating new objects.
        var oldLeftVS = $scope.state.left.vs;
        if (oldLeftVS !== undefined) {
          $scope.state.left.vs = { id: oldLeftVS.id };
        }
        var oldRightVS = $scope.state.right.vs;
        if (oldRightVS !== undefined) {
          $scope.state.right.vs = { id: oldRightVS.id };
        }
        loadStartingVertexSets();
      }
    }

    function applyOperationFlow(operation) {
      openOperationModal(operation).then(function(modalResult) {
        applyOperation(operation, modalResult);
      });
    }

    var StartingOps = $resource('/ajax/startingOps');
    $scope.startingOps = StartingOps.query({q: {fake: 0}});
    loadStartingVertexSets();

    $scope.apply = applyOperationFlow;
    $scope.removePath = function() {
      $scope.state.leftToRightPath = undefined;
    };

    $scope.left.state = function() {
      return $scope.state.left;
    };
    $scope.right.state = function() {
      return $scope.state.right;
    };
    $scope.setState = function(side, setting, value) {
      if (side.state()[setting] === value) {
        // Clicking the same setting again turns it off.
        delete side.state()[setting];
      } else {
        side.state()[setting] = value;
      }
    };

    $scope.left.name = 'left';
    $scope.right.name = 'right';

    // Creates a new side state, while retaining the settings where this makes sense.
    function freshSideState(old) {
      var fresh = defaultSideState();
      fresh.graphMode = old.graphMode;
      fresh.sampleRadius = old.sampleRadius;
      fresh.bucketCount = old.bucketCount;
      return fresh;
    }
    $scope.left.setVS = function(id) {
      $scope.state.left = freshSideState($scope.state.left);
      $scope.state.left.vs = { id: id };
    };
    $scope.right.setVS = function(id) {
      $scope.state.right = freshSideState($scope.state.right);
      $scope.state.right.vs = { id: id };
    };

    $scope.mirror = function(side) {
      $scope.state.leftToRightPath = [];
      side.other.setVS(side.data.id);
    };

    $scope.close = function(side) {
      $scope.removePath();
      side.state().vs = undefined;
    };

    function setNewVS(side) {
      return function(id) {
        $scope.removePath();
        side.setVS(id);
      };
    }
    $scope.left.setNewVS = setNewVS($scope.left);
    $scope.right.setNewVS = setNewVS($scope.right);

    $scope.left.addEBToPath = function(bundle, pointsTowardsMySide) {
      $scope.state.leftToRightPath.unshift({bundle: bundle, pointsLeft: pointsTowardsMySide});
    };
    $scope.right.addEBToPath = function(bundle, pointsTowardsMySide) {
      $scope.state.leftToRightPath.push({bundle: bundle, pointsLeft: !pointsTowardsMySide});
    };

    function followEB(side) {
      return function(bundle, pointsTowardsMySide) {
        if ($scope.state.leftToRightPath !== undefined) {
          side.addEBToPath(bundle, pointsTowardsMySide);
        }
        var oldFilters = side.state().filters;
        if (pointsTowardsMySide) {
          side.setVS(bundle.destination.id);
        } else {
          side.setVS(bundle.source.id);
        }
        // Keep filters when following local edges.
        if (bundle.destination.id === bundle.source.id) {
          side.state().filters = oldFilters;
        }
      };
    }
    $scope.left.followEB = followEB($scope.left);
    $scope.right.followEB = followEB($scope.right);

    function showEB(side) {
      return function(bundle, pointsTowardsMySide) {
        $scope.state.leftToRightPath = [];
        side.addEBToPath(bundle, pointsTowardsMySide);
        if (pointsTowardsMySide) {
          side.other.setVS(bundle.source.id);
        } else {
          side.other.setVS(bundle.destination.id);
        }
      };
    }
    $scope.left.showEB = showEB($scope.left);
    $scope.right.showEB = showEB($scope.right);

    $scope.cutPathLeft = function(idx) {
      $scope.state.leftToRightPath.splice(0, idx);
      var firstStep = $scope.state.leftToRightPath[0];
      if (firstStep.pointsLeft) {
        $scope.left.setVS(firstStep.bundle.destination.id);
      } else {
        $scope.left.setVS(firstStep.bundle.source.id);
      }
    };
    $scope.cutPathRight = function(idx) {
      $scope.state.leftToRightPath.splice(idx + 1);
      var lastStep = $scope.state.leftToRightPath[$scope.state.leftToRightPath.length - 1];
      if (lastStep.pointsLeft) {
        $scope.right.setVS(lastStep.bundle.source.id);
      } else {
        $scope.right.setVS(lastStep.bundle.destination.id);
      }
    };
  });
