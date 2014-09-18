'use strict';

angular.module('biggraph').directive('projectGraph', function (util) {
  return {
    restrict: 'E',
    scope: { left: '=', right: '=', leftToRightPath: '=' },
    replace: false,
    templateUrl: 'project-graph.html',
    link: function(scope) {
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
        if (sides.length === 0) {  // Nothing to draw.
          scope.graphView = undefined;
          return;
        }
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
            xBucketingAttributeId: viewData.xAttribute.id || '',
            yBucketingAttributeId: viewData.yAttribute.id || '',
            xNumBuckets: parseInt(viewData.bucketCount),  // angular.js/pull/7370
            yNumBuckets: parseInt(viewData.bucketCount),  // angular.js/pull/7370
            // Sampled view parameters.
            radius: parseInt(viewData.sampleRadius),  // angular.js/pull/7370
            centralVertexIds: viewData.centers,
            sampleSmearEdgeBundleId: (viewData.edgeBundle || { id: '' }).id,
            labelAttributeId: viewData.labelAttribute.id || '',
            sizeAttributeId: viewData.sizeAttribute.id || '',
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

      scope.$watch('graphView.$resolved', function() {
        // Generate the TSV representation.
        scope.tsv = '';
        var gv = scope.graphView;
        if (!gv || !gv.$resolved) {
          return;
        }
        var sides = [scope.left, scope.right];
        var vsIndex = 0;
        for (var i = 0; i < sides.length; ++i) {
          if (sides[i] && sides[i].graphMode) {
            scope.tsv += vertexSetToTSV(i, gv.vertexSets[vsIndex], sides[i]);
            vsIndex += 1;
          }
        }
        for (i = 0; i < gv.edgeBundles.length; ++i) {
          scope.tsv += edgeBundleToTSV(gv.edgeBundles[i]);
        }
      });

      function vertexSetToTSV(index, vs, side) {
        var i, j, v;
        var name = graphName(index);
        var tsv = '\n';
        if (vs.mode === 'sampled') {
          tsv += 'Vertices of ' + name + ':\n';
          tsv += 'id';
          if (side.labelAttribute.id) { tsv += '\t' + side.labelAttribute.title; }
          if (side.sizeAttribute.id) { tsv += '\t' + side.sizeAttribute.title; }
          tsv += '\n';
          for (i = 0; i < vs.vertices.length; ++i) {
            v = vs.vertices[i];
            tsv += v.id;
            if (side.labelAttribute.id) { tsv += '\t' + v.label; }
            if (side.sizeAttribute.id) { tsv += '\t' + v.size; }
            tsv += '\n';
          }
        } else {
          var xAxis = side.xAttribute.title;
          var yAxis = side.yAttribute.title;
          var xDescription =
            xAxis + ' (horizontal' + (vs.xLabelType === 'between' ? ', lower bounds' : '') + ')';
          var yDescription =
            yAxis + ' (vertical' + (vs.yLabelType === 'between' ? ', lower bounds' : '') + ')';
          tsv += 'Buckets of ' + name;
          if (xAxis && yAxis) {
            tsv += ' by ' + yDescription + ' and ' + xDescription + ':\n';
          } else if (xAxis) {
            tsv += ' by ' + xDescription + ':\n';
          } else if (yAxis) {
            tsv += ' by ' + yDescription + ':\n';
          } else {
            tsv += ':\n';
          }
          var byBucket = {};
          for (i = 0; i < vs.vertices.length; ++i) {
            v = vs.vertices[i];
            byBucket[v.x + ', ' + v.y] = v;
          }
          var xl = vs.xLabelType === 'between' ? vs.xLabels.length - 1 : vs.xLabels.length;
          var yl = vs.yLabelType === 'between' ? vs.yLabels.length - 1 : vs.yLabels.length;
          for (j = 0; j < vs.xLabels.length; ++j) {
            // X-axis header.
            tsv += '\t' + vs.xLabels[j];
          }
          tsv += '\n';
          for (j = 0; j < vs.yLabels.length; ++j) {
            tsv += vs.yLabels[j];  // Y-axis header.
            for (i = 0; j < yl && i < xl; ++i) {
              tsv += '\t' + byBucket[i + ', ' + j].size;
            }
            tsv += '\n';
          }
        }
        return tsv;
      }

      function edgeBundleToTSV(eb) {
        var i, j;
        var tsv = '\n';
        tsv += 'Edges from ' + graphName(eb.srcIdx) + ' (vertical) to ' + graphName(eb.dstIdx) + ' (horizontal):\n';
        // A simple dump. Adding the vertex indices would not make it clearer.
        var maxA = 0, maxB = 0;
        var byPair = {};
        for (i = 0; i < eb.edges.length; ++i) {
          var e = eb.edges[i];
          byPair[e.a + ', ' + e.b] = e.size;
          if (e.a > maxA) { maxA = e.a; }
          if (e.b > maxB) { maxB = e.b; }
        }
        for (j = 0; j <= maxB; ++j) {
          for (i = 0; i <= maxA; ++i) {
            tsv += (byPair[i + ', ' + j] || 0) + (i === maxA ? '\n' : '\t');
          }
        }
        return tsv;
      }

      function graphName(index) {
        return ['the left-side graph', 'the right-side graph'][index] || 'graph ' + (index + 1);
      }
    }
  };
});
