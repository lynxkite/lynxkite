// Provides a class that can construct and send diagram requests.
'use strict';

angular.module('biggraph').factory('loadGraph', function (util) {
  function Graph() {
    this.request = undefined;
  }

  Graph.prototype.load = function(left, right, leftToRightBundle, rightToLeftBundle) {
    // This indirection makes it certain that the returned graph is consistent.
    this.left = angular.copy(left, this.left);
    this.right = angular.copy(right, this.right);

    var sides = [];
    if (left && left.graphMode && left.vertexSet !== undefined) {
      sides.push(left);
    }
    if (right && right.graphMode && right.vertexSet !== undefined) {
      sides.push(right);
    }
    if (sides.length === 0) {  // Nothing to draw.
      return;
    }
    var q = { vertexSets: [], edgeBundles: [] };
    for (var i = 0; i < sides.length; ++i) {
      var viewData = sides[i];
      if (viewData.edgeBundle !== undefined) {
        var edgeAttrs = [];
        for (var eIndex in viewData.edgeAttrs) {
          if (viewData.edgeAttrs[eIndex]) {
            edgeAttrs.push({
              attributeId: viewData.edgeAttrs[eIndex].id,
              aggregator: viewData.edgeAttrs[eIndex].aggregator,
            });
          }
        }
        edgeAttrs.sort();
        q.edgeBundles.push({
          srcDiagramId: 'idx[' + i + ']',
          dstDiagramId: 'idx[' + i + ']',
          srcIdx: i,
          dstIdx: i,
          edgeBundleId: viewData.edgeBundle.id,
          filters: viewData.filters.edge,
          edgeWeightId: (viewData.edgeWidth || { id: '' }).id,
          layout3D: viewData.display === '3d',
          relativeEdgeDensity: viewData.relativeEdgeDensity,
          attrs: edgeAttrs,
          maxSize: (viewData.display === '3d') ? 1000000 : 10000,
        });
      }
      var vertexAttrs = [];
      for (var index in viewData.vertexAttrs) {
        if (viewData.vertexAttrs[index]) {
          vertexAttrs.push(viewData.vertexAttrs[index].id);
        }
      }
      vertexAttrs.sort();
      var xAttr = (viewData.xAttribute) ? viewData.xAttribute.id : '';
      var yAttr = (viewData.yAttribute) ? viewData.yAttribute.id : '';

      q.vertexSets.push({
        vertexSetId: viewData.vertexSet.id,
        filters: viewData.filters.vertex,
        mode: viewData.graphMode,
        // Bucketed view parameters.
        xBucketingAttributeId: xAttr,
        yBucketingAttributeId: yAttr,
        xNumBuckets: viewData.bucketCount,
        yNumBuckets: viewData.bucketCount,
        xAxisOptions: viewData.xAxisOptions,
        yAxisOptions: viewData.yAxisOptions,
        sampleSize: viewData.preciseBucketSizes ? -1 : 50000,
        // Sampled view parameters.
        // angular.js/pull/7370
        radius: viewData.edgeBundle ? viewData.sampleRadius : 0,
        centralVertexIds: viewData.centers,
        sampleSmearEdgeBundleId: (viewData.edgeBundle || { id: '' }).id,
        attrs: vertexAttrs,
        maxSize: (viewData.display === '3d') ? 1000000 : 10000,
      });
    }

    if (sides.length === 2 && sides[0].display === 'svg' && sides[1].display === 'svg') {
      if (leftToRightBundle !== undefined) {
        q.edgeBundles.push({
          srcDiagramId: 'idx[0]',
          dstDiagramId: 'idx[1]',
          srcIdx: 0,
          dstIdx: 1,
          edgeBundleId: leftToRightBundle,
          filters: [],
          edgeWeightId: '',
          layout3D: false,
          relativeEdgeDensity: false,
          attrs: [],
          maxSize: 10000,
        });
      }
      if (rightToLeftBundle !== undefined) {
        q.edgeBundles.push({
          srcDiagramId: 'idx[1]',
          dstDiagramId: 'idx[0]',
          srcIdx: 1,
          dstIdx: 0,
          edgeBundleId: rightToLeftBundle,
          filters: [],
          edgeWeightId: '',
          layout3D: false,
          relativeEdgeDensity: false,
          attrs: [],
          maxSize: 10000,
        });
      }
    }

    if (!angular.equals(this.request, q)) {
      this.request = angular.copy(q);  // Store request without any references to living objects.
      this.view = util.get('/ajax/complexView', this.request);
    }
  };
  return { Graph: Graph };
});
