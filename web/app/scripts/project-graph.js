'use strict';

angular.module('biggraph').directive('projectGraph', function (util) {
  return {
    restrict: 'E',
    scope: { left: '=', right: '=', leftToRightPath: '=' },
    replace: false,
    templateUrl: 'project-graph.html',
    link: function(scope) {
      scope.showGraph = function() {
        return (scope.left && scope.left.graphMode) || (scope.right && scope.right.graphMode);
      };

      util.deepWatch(scope, 'left', update);
      util.deepWatch(scope, 'right', update);

      function update(after, before) {
        if (after && before && before !== after) {
          before.animate = after.animate;
          if (angular.equals(before, after)) {
            // The only difference is in the "animate" setting. Do not reload.
            return;
          }
        }

        var sides = [];
        if (scope.left && scope.left.graphMode && scope.left.vertexSet !== undefined) {
            sides.push(scope.left);
        }
        if (scope.right && scope.right.graphMode && scope.right.vertexSet !== undefined) {
            sides.push(scope.right);
        }
        if (sides.length === 0) { return; }
        var q = { vertexSets: [], edgeBundles: [] };
        for (var i = 0; i < sides.length; ++i) {
          var viewData = sides[i];
          if (viewData.edgeBundle !== undefined) {
            q.edgeBundles.push({
              srcDiagramId: 'idx[' + i + ']',
              dstDiagramId: 'idx[' + i + ']',
              srcIdx: i,
              dstIdx: i,
              bundleSequence: [{ bundle: viewData.edgeBundle.id, reversed: false }]
            });
          }
          var filters = [];
          for (var attr in viewData.filters) {
            if (viewData.filters[attr] !== '') {
              filters.push({ attributeId: attr, valueSpec: viewData.filters[attr] });
            }
          }
          q.vertexSets.push({
            vertexSetId: viewData.vertexSet.id,
            filters: filters,
            mode: viewData.graphMode,
            // Bucketed view parameters.
            xBucketingAttributeId: viewData.xAttribute || '',
            yBucketingAttributeId: viewData.yAttribute || '',
            xNumBuckets: parseInt(viewData.bucketCount),  // angular.js/pull/7370
            yNumBuckets: parseInt(viewData.bucketCount),  // angular.js/pull/7370
            // Sampled view parameters.
            radius: parseInt(viewData.sampleRadius),  // angular.js/pull/7370
            centralVertexIds: viewData.center,
            sampleSmearEdgeBundleId: (viewData.edgeBundle || { id: '' }).id,
            labelAttributeId: viewData.labelAttribute || '',
            sizeAttributeId: viewData.sizeAttribute || '',
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
