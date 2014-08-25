'use strict';

angular.module('biggraph').directive('projectGraph', function ($resource, util) {
  return {
    restrict: 'E',
    scope: { left: '=', right: '=', leftToRightPath: '=' },
    replace: false,
    templateUrl: 'project-graph.html',
    link: function(scope) {      
      scope.showGraph = function() {
        var leftViewData = scope.left.getViewData();
        var rightViewData = scope.right.getViewData();
        return (leftViewData && leftViewData.graphMode) || (rightViewData && rightViewData.graphMode);
      };

      util.deepWatch(scope, 'left.state', update);
      util.deepWatch(scope, 'right.state', update);

      function update() {
        var leftViewData = scope.left.getViewData();
        var rightViewData = scope.right.getViewData();        
        var sides = [];
        if (leftViewData && leftViewData.graphMode && (leftViewData.vertexSet !== undefined)) {
          sides.push(leftViewData);
        }
        if (rightViewData && rightViewData.graphMode && (rightViewData.vertexSet !== undefined)) {
          sides.push(rightViewData);
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
            centralVertexId: (viewData.center || '').toString(),
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
