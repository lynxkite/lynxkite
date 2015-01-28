'use strict';

angular.module('biggraph').directive('projectGraph', function (util) {
  return {
    restrict: 'E',
    scope: {
      left: '=',
      right: '=',
      leftToRightBundle: '=',
      rightToLeftBundle: '=',
      contextMenu: '=' },
    replace: false,
    templateUrl: 'project-graph.html',
    link: function(scope, element) {
      util.deepWatch(scope, 'left', updateRequest);
      util.deepWatch(scope, 'right', updateRequest);
      util.deepWatch(scope, 'leftToRightBundle', updateRequest);
      util.deepWatch(scope, 'rightToLeftBundle', updateRequest);

      scope.onIconsLoaded = function() {
        scope.$broadcast('#svg-icons is loaded');
      };

      scope.graph = {};
      function updateRequest() {
        // This indirection makes it certain that graph-view does not see more recent data than
        // project-graph does.
        scope.graph.left = angular.copy(scope.left, scope.graph.left);
        scope.graph.right = angular.copy(scope.right, scope.graph.right);

        scope.global = false;
        var sides = [];
        if (scope.left && scope.left.graphMode && scope.left.vertexSet !== undefined) {
          sides.push(scope.left);
        }
        if (scope.right && scope.right.graphMode && scope.right.vertexSet !== undefined) {
          sides.push(scope.right);
        }
        if (sides.length === 0) {  // Nothing to draw.
          scope.request = undefined;
          return;
        }
        var q = { vertexSets: [], edgeBundles: [] };
        for (var i = 0; i < sides.length; ++i) {
          var viewData = sides[i];
          if (viewData.graphMode === 'global') {
            // TODO: Make this a per-side setting.
            scope.global = true;
            q = {
              vertexSetId: viewData.vertexSet.id,
              edgeBundleId: viewData.edgeBundle.id,
            };
            break;
          }
          if (viewData.edgeBundle !== undefined) {
            q.edgeBundles.push({
              srcDiagramId: 'idx[' + i + ']',
              dstDiagramId: 'idx[' + i + ']',
              srcIdx: i,
              dstIdx: i,
              edgeBundleId: viewData.edgeBundle.id,
              filters: viewData.filters.edge,
              edgeWeightId: (viewData.edgeWidth || { id: '' }).id
            });
          }
          // we sort attributes by UUID to avoid recomputing the same combination
          var attrs = [];
          for (var index in viewData.attrs) {
            if (viewData.attrs[index]) {
              attrs.push(viewData.attrs[index].id);
            }
          }
          attrs.sort();
          var xAttr = (viewData.xAttribute) ? viewData.xAttribute.id : '';
          var yAttr = (viewData.yAttribute) ? viewData.yAttribute.id : '';

          q.vertexSets.push({
            vertexSetId: viewData.vertexSet.id,
            filters: viewData.filters.vertex,
            mode: viewData.graphMode,
            // Bucketed view parameters.
            xBucketingAttributeId: xAttr,
            yBucketingAttributeId: yAttr,
            xNumBuckets: parseInt(viewData.bucketCount),  // angular.js/pull/7370
            yNumBuckets: parseInt(viewData.bucketCount),  // angular.js/pull/7370
            xAxisOptions: viewData.xAxisOptions,
            yAxisOptions: viewData.yAxisOptions,
            // Sampled view parameters.
            // angular.js/pull/7370
            radius: viewData.edgeBundle ? parseInt(viewData.sampleRadius) : 0,
            centralVertexIds: viewData.centers,
            sampleSmearEdgeBundleId: (viewData.edgeBundle || { id: '' }).id,
            attrs: attrs,
          });
        }
        if (sides.length === 2 && scope.leftToRightBundle !== undefined) {
          q.edgeBundles.push({
            srcDiagramId: 'idx[0]',
            dstDiagramId: 'idx[1]',
            srcIdx: 0,
            dstIdx: 1,
            edgeBundleId: scope.leftToRightBundle,
            filters: [],
            edgeWeightId: '',
          });
        }
        if (sides.length === 2 && scope.rightToLeftBundle !== undefined) {
          q.edgeBundles.push({
            srcDiagramId: 'idx[1]',
            dstDiagramId: 'idx[0]',
            srcIdx: 1,
            dstIdx: 0,
            edgeBundleId: scope.rightToLeftBundle,
            filters: [],
            edgeWeightId: '',
          });
        }
        if (!angular.equals(scope.request, q)) {
          scope.request = q;
          if (scope.global) {
            scope.graph.view = util.get('/ajax/globalView', scope.request);
          } else {
            scope.graph.view = util.get('/ajax/complexView', scope.request);
          }
        }
      }

      scope.$watch('graph.view', updateTSV);
      scope.$watch('graph.view.$resolved', updateTSV);
      function updateTSV() {
        // Generate the TSV representation.
        scope.tsv = '';
        var gv = scope.graph.view;
        if (!gv || !gv.$resolved || gv.$error || scope.global) {
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
      }

      scope.contextMenu = {
        enabled: false,
        x: 0,
        y: 0,
        data: {}
      };

      function vertexSetToTSV(index, vs, side) {
        var i, j, v;
        var name = graphName(index);
        var tsv = '\n';
        if (vs.mode === 'sampled') {
          tsv += 'Vertices of ' + name + ':\n';
          tsv += 'id';
          var attrs = [];
          angular.forEach(side.attrs, function(attr) { if (attr) { attrs.push(attr); } });
          for (i = 0; i < attrs.length; ++i) {
            tsv += '\t' + attrs[i].title;
          }
          tsv += '\n';
          for (i = 0; i < vs.vertices.length; ++i) {
            v = vs.vertices[i];
            tsv += v.id;
            for (j = 0; j < attrs.length; ++j) {
              tsv += '\t' + v.attrs[attrs[j].id].string;
            }
            tsv += '\n';
          }
        } else {
          var xAxis = side.xAttribute || {};
          var yAxis = side.yAttribute || {};
          var xDescription = xAxis.title + ' (horizontal';
          if (vs.xLabelType === 'between') { xDescription += ', lower bounds'; }
          xDescription += ')';
          var yDescription = yAxis.title + ' (vertical';
          if (vs.yLabelType === 'between') { yDescription += ', lower bounds'; }
          yDescription += ')';
          tsv += 'Buckets of ' + name;
          if (xAxis.id && yAxis.id) {
            tsv += ' by ' + yDescription + ' and ' + xDescription + ':\n';
          } else if (xAxis.id) {
            tsv += ' by ' + xDescription + ':\n';
          } else if (yAxis.id) {
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
        var tsv = '\n';
        tsv += 'Edges from ' + graphName(eb.srcIdx);
        tsv += ' to ' + graphName(eb.dstIdx) + ':\n';
        tsv += 'src\tdst\tsize\n';
        for (var i = 0; i < eb.edges.length; ++i) {
          var e = eb.edges[i];
          tsv += e.a + '\t' + e.b + '\t' + e.size + '\n';
        }
        return tsv;
      }

      function graphName(index) {
        return ['the left-side graph', 'the right-side graph'][index] || 'graph ' + (index + 1);
      }

      function updateFilters() {
        var svg = element.find('svg.graph-view');
        var filter = '';
        if (scope.filters.inverted) {
          filter += 'invert(100%) hue-rotate(180deg) ';
        }
        filter += 'contrast(' + scope.filters.contrast + '%) ';
        filter += 'saturate(' + scope.filters.saturation + '%) ';
        filter += 'brightness(' + scope.filters.brightness + '%) ';
        svg.css({ filter: filter, '-webkit-filter': filter });
      }
      function saveFilters() {
        window.localStorage.setItem('graph-filters', JSON.stringify(scope.filters));
      }
      scope.resetFilters = function() {
        scope.filters = { inverted: false, contrast: 100, saturation: 100, brightness: 100 };
      };
      scope.resetFilters();
      var loadedFilters = window.localStorage.getItem('graph-filters');
      if (loadedFilters) {
        angular.extend(scope.filters, JSON.parse(loadedFilters));
      }
      util.deepWatch(scope, 'filters', function() { saveFilters(); updateFilters(); });
    },
  };
});
