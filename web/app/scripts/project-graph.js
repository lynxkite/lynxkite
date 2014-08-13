'use strict';

angular.module('biggraph').directive('projectGraph', function ($resource, util) {
  return {
    restrict: 'E',
    scope: { left: '=', right: '=', leftToRightPath: '=' },
    replace: false,
    templateUrl: 'project-graph.html',
    link: function(scope) {
      scope.showGraph = function() { return scope.left.graphMode || scope.right.graphMode; };

      util.deepWatch(scope, 'left', update);
      util.deepWatch(scope, 'right', update);

      function update() {
        if (!scope.showGraph()) { return; }
        var sides = [];
        if (scope.left.graphMode && (scope.left.vertexSet !== undefined)) {
          sides.push(scope.left);
        }
        if (scope.right.graphMode && (scope.right.vertexSet !== undefined)) {
          sides.push(scope.right);
        }
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
            vertexSetId: side.vertexSet.id,
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
        if (sides.length === 2 && scope.leftToRightPath !== undefined) {
          var bundles = scope.leftToRightPath.map(function(step) {
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
        scope.graphView = util.get('/ajax/complexView', q);
      }
    }
  };
});
