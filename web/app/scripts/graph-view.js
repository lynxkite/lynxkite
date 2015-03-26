'use strict';

angular.module('biggraph').directive('graphView', function(util, $compile, $timeout) {
  /* global SVG_UTIL, COMMON_UTIL, FORCE_LAYOUT, tinycolor */
  var svg = SVG_UTIL;
  var common = COMMON_UTIL;
  var directive = {
      template: '<svg class="graph-view" version="1.1" xmlns="http://www.w3.org/2000/svg"></svg>',
      scope: { graph: '=', menu: '=' },
      replace: true,
      link: function(scope, element) {
        scope.gv = new GraphView(scope, element);
        function updateGraph() {
          if (scope.graph.view === undefined || !scope.graph.view.$resolved || !iconsLoaded()) {
            scope.gv.loading();
          } else if (scope.graph.view.$error) {
            scope.gv.error(scope.graph.view);
          } else {
            scope.gv.update(scope.graph.view, scope.menu);
          }
        }
        scope.$watch('graph.view', updateGraph);
        scope.$watch('graph.view.$resolved', updateGraph);
        // An attribute change can happen without a graph data change. Watch them separately.
        // (When switching from "color" to "slider", for example.)
        util.deepWatch(scope, 'graph.left.vertexAttrs', updateGraph);
        util.deepWatch(scope, 'graph.right.vertexAttrs', updateGraph);
        util.deepWatch(scope, 'graph.left.edgeAttrs', updateGraph);
        util.deepWatch(scope, 'graph.right.edgeAttrs', updateGraph);
        // It is possible, especially in testing, that we get the graph data faster than the icons.
        // In this case we delay the drawing until the icons are loaded.
        scope.$on('#svg-icons is loaded', updateGraph);
      },
    };

  function iconsLoaded() {
    return angular.element('#svg-icons #circle').length > 0;
  }

  function getIcon(name) {
    if (!name) { name = 'circle'; }
    var circle = angular.element('#svg-icons #circle');
    var cbb = circle[0].getBBox();
    var icon = angular.element('#svg-icons #' + name.toLowerCase());
    if (icon.length === 0) { icon = circle; }
    var bb = icon[0].getBBox();
    var clone = icon.clone();
    // Take the scaling factor from the circle icon.
    clone.scale = 2 / Math.max(cbb.width, cbb.height);
    clone.center = {
      x: bb.x + bb.width / 2,
      y: bb.y + bb.height / 2,
    };
    return clone;
  }

  function Offsetter(xOff, yOff, zoom, thickness, menu) {
    this.xOff = xOff;
    this.yOff = yOff;
    this.zoom = zoom;  // Zoom for positions.
    this.thickness = thickness;  // Zoom for radius/width.
    this.menu = menu;
    this.elements = [];
  }
  Offsetter.prototype.rule = function(element) {
    this.elements.push(element);
    var that = this;
    element.offsetter = this;
    element.screenX = function() {
      return element.x * that.zoom + that.xOff;
    };
    element.screenY = function() {
      return element.y * that.zoom + that.yOff;
    };
    element.activateMenu = function(menuData) {
      that.menu.x = element.screenX();
      that.menu.y = element.screenY();
      that.menu.data = menuData;
      that.menu.enabled = true;
    };
    element.reDraw();
  };
  Offsetter.prototype.panTo = function(x, y) {
    this.xOff = x;
    this.yOff = y;
    this.reDraw();
  };
  Offsetter.prototype.reDraw = function() {
    for (var i = 0; i < this.elements.length; ++i) {
      this.elements[i].reDraw();
    }
  };
  Offsetter.prototype.inherit = function() {
    var offsetter = new Offsetter(this.xOff, this.yOff, this.zoom, this.thickness, this.menu);
    offsetter.inherited = true;
    return offsetter;
  };

  function GraphView(scope, element) {
    this.scope = scope;
    this.unregistration = [];  // Cleanup functions to be called before building a new graph.
    this.svg = angular.element(element);
    this.svg.append([
      svg.marker('arrow'),
      svg.marker('arrow-highlight-in'),
      svg.marker('arrow-highlight-out'),
    ]);
    this.root = svg.create('g', {'class': 'root'});
    this.svg.append(this.root);
    // Top-level mouse/touch listeners.
    this.svgMouseDownListeners = [];
    var that = this;
    this.svg.on('mousedown touchstart', function(e) {
      for (var i = 0; i < that.svgMouseDownListeners.length; ++i) {
        that.svgMouseDownListeners[i](e);
      }
    });
    this.svgMouseWheelListeners = [];
    this.svg.on('wheel', function(e) {
      for (var i = 0; i < that.svgMouseWheelListeners.length; ++i) {
        that.svgMouseWheelListeners[i](e);
      }
    });
    this.svgDoubleClickListeners = [];
    function doubleClick(e) {
      for (var i = 0; i < that.svgDoubleClickListeners.length; ++i) {
        that.svgDoubleClickListeners[i](e);
      }
    }
    this.svg.on('dblclick', doubleClick);
    // Handle right double-clicks too. This disables the default context
    // menu, which is actually a good thing too.
    var lastRightClickTime = 0;
    this.svg.on('contextmenu', function(e) {
      e.preventDefault();
      var now = Date.now();
      if (now - lastRightClickTime < 300) {  // milliseconds
        doubleClick(e);
        now = 0;
      }
      lastRightClickTime = now;
    });
    this.renderers = [];  // 3D renderers.
  }

  GraphView.prototype.clear = function() {
    svg.removeClass(this.svg, 'loading');
    svg.removeClass(this.svg, 'fade-non-opaque');
    this.root.empty();
    for (var i = 0; i < this.unregistration.length; ++i) {
      this.unregistration[i]();
    }
    this.unregistration = [];
    this.svgMouseDownListeners = [];
    this.svgMouseWheelListeners = [];
    this.svgDoubleClickListeners = [];
    for (i = 0; i < this.renderers.length; ++i) {
      this.renderers[i].scope().$destroy();
      this.renderers[i].remove();
    }
    this.renderers = [];
  };

  GraphView.prototype.loading = function() {
    svg.addClass(this.svg, 'loading');
  };

  GraphView.prototype.error = function(view) {
    this.clear();
    var x = this.svg.width() / 2, y = this.svg.height() / 2;
    var text = svg.create('text', {'class': 'clicky error', x: x, y: y, 'text-anchor': 'middle'});
    text.text('Failed to generate visualization. Click to report problem.');
    text.click(function() {
      util.reportRequestError(view, 'Graph visualization failed.');
    });
    this.root.append(text);
  };

  var graphToSVGRatio = 0.8;  // Leave some margin.
  var UNCOLORED = 'hsl(0,0%,42%)';

  GraphView.prototype.update = function(data, menu) {
    this.clear();
    var zoom = this.svg.height() * graphToSVGRatio;
    var sides = [this.scope.graph.left, this.scope.graph.right];
    this.edgeGroup = svg.create('g', {'class': 'edges'});
    this.vertexGroup = svg.create('g', {'class': 'nodes'});
    this.legend = svg.create('g', {'class': 'legend'});
    this.legendNextLine = 0;
    this.root.append([this.edgeGroup, this.vertexGroup, this.legend]);
    var oldVertices = this.vertices || [];
    this.vertices = [];  // Sparse, indexed by side.
    var vsIndices = [];  // Packed, indexed by position in the JSON.
    var vsIndex = 0;
    var halfColumnWidth = this.svg.width() / sides.length / 2;
    var i, vs;
    for (i = 0; i < sides.length; ++i) {
      if (sides[i] && sides[i].graphMode) {
        var xMin = (i * 2) * halfColumnWidth;
        var xOff = (i * 2 + 1) * halfColumnWidth;
        var xMax = (i * 2 + 2) * halfColumnWidth;
        var yOff = this.svg.height() / 2;
        var dataVs = data.vertexSets[vsIndex];
        vsIndex += 1;
        vsIndices.push(i);
        if (sides[i].display === '3d') { continue; }
        var offsetter;
        if (oldVertices[i] && oldVertices[i].mode === dataVs.mode) {
          offsetter = oldVertices[i].offsetter.inherit();
        } else {
          offsetter = new Offsetter(xOff, yOff, zoom, zoom, menu);
        }
        if (dataVs.mode === 'sampled') {
          vs = this.addSampledVertices(dataVs, offsetter, sides[i]);
        } else {
          vs = this.addBucketedVertices(dataVs, offsetter, sides[i]);
        }
        vs.offsetter = offsetter;
        vs.xMin = xMin;
        vs.halfColumnWidth = halfColumnWidth;
        this.vertices[i] = vs;
        this.sideMouseBindings(offsetter, xMin, xMax);
      }
    }
    var side;
    for (i = 0; i < data.edgeBundles.length; ++i) {
      var e = data.edgeBundles[i];
      // Avoid an error with the Grunt test data, which has edges going to the other side
      // even if we only have one side.
      if (e.srcIdx >= vsIndex || e.dstIdx >= vsIndex) { continue; }
      side = undefined;
      if (e.srcIdx === e.dstIdx) {
        side = sides[vsIndices[e.srcIdx]];
        if (side.display === '3d') {
          var scope = this.scope.$new();
          scope.edges = e.edges;
          scope.layout3D = e.layout3D;
          scope.width = 2 * halfColumnWidth;
          scope.left = vsIndices[e.srcIdx] * 2 * halfColumnWidth;
          var r = $compile('<renderer></renderer>')(scope);
          this.svg.after(r);
          this.renderers.push(r);
          continue;
        }
      }
      var src = this.vertices[vsIndices[e.srcIdx]];
      var dst = this.vertices[vsIndices[e.dstIdx]];
      var edges = this.addEdges(e.edges, src, dst, side);
      if (e.srcIdx === e.dstIdx) {
        src.edges = edges;
      }
    }
    for (i = 0; i < this.vertices.length; ++i) {
      vs = this.vertices[i];
      if (vs && vs.mode === 'sampled') {
        var old = oldVertices[i];
        if (old && old.vertexSetId === vs.vertexSetId) {
          copyLayoutAndFreezeOld(old, vs);
        }
        this.initSampled(vs);
        unfreezeAll(vs);
      }
    }
  };

  function copyLayoutAndFreezeOld(from, to) {
    var fromById = {};
    for (var i = 0; i < from.length; ++i) {
      fromById[from[i].id] = from[i];
    }
    for (i = 0; i < to.length; ++i) {
      var v = to[i];
      var fv = fromById[v.id];
      if (fv) {
        v.x = fv.x;
        v.y = fv.y;
        // Copy frozen status, plus add one more freeze.
        v.frozen = fv.frozen + 1;
      }
    }
  }

  function unfreezeAll(vs) {
    for (var i = 0; i < vs.length; ++i) {
      if (vs[i].frozen) {
        vs[i].frozen -= 1;
      }
    }
  }

  function mapByAttr(vs, attr, type) {
    return vs.filter(function(v) {
      return v.attrs[attr].defined;
    }).map(function(v) {
      return v.attrs[attr][type];
    });
  }

  function doubleColorMap(values) {
    var bounds = common.minmax(values);
    var colorMap = {};
    for (var i = 0; i < values.length; ++i) {
      var h = 300 + common.normalize(values[i], bounds) * 120;
      colorMap[values[i]] = 'hsl(' + h + ',50%,42%)';
    }
    return colorMap;
  }

  function stringColorMap(values) {
    var i, set = {};
    for (i = 0; i < values.length; ++i) {
      set[values[i]] = 1;
    }
    var keys = Object.keys(set);
    keys.sort();  // This provides some degree of stability.
    var colorMap = {};
    for (i = 0; i < keys.length; ++i) {
      var h = Math.floor(360 * i / keys.length);
      colorMap[keys[i]] = 'hsl(' + h + ',50%,42%)';
    }
    // Strings that are valid CSS color names will be used as they are.
    // To identify them we have to try setting them as color on a hidden element.
    var colorTester = angular.element('#svg-icons')[0];
    for (i = 0; i < keys.length; ++i) {
      colorTester.style.color = 'transparent';
      colorTester.style.color = keys[i];
      if (colorTester.style.color !== 'transparent') {
        colorMap[keys[i]] = keys[i];
      }
    }
    return colorMap;
  }

  GraphView.prototype.setupColorMap = function(
      siblings, colorMeta, sideString, legendTitle, colorKey) {
    var resultMap;
    if (colorMeta) {
      colorKey = (colorKey === undefined) ? colorMeta.id : colorKey;
      if (colorMeta.typeName === 'Double') {
        var values = mapByAttr(siblings, colorKey, 'double');
        resultMap = doubleColorMap(values);
        var bounds = common.minmax(values);
        var legendMap = {};
        legendMap['min: ' + bounds.min] = resultMap[bounds.min];
        legendMap['max: ' + bounds.max] = resultMap[bounds.max];
        // only shows the min max values
        this.addColorLegend(legendMap, sideString, legendTitle);
      } else if (colorMeta.typeName === 'String') {
        resultMap = stringColorMap(mapByAttr(siblings, colorKey, 'string'));
        this.addColorLegend(resultMap, sideString, legendTitle);
      } else {
        console.error('The type of ' +
                      colorMeta + ' (' + colorMeta.typeName +
                      ') is not supported for color visualization!');
      }
    }
    return resultMap;
  };

  GraphView.prototype.addSampledVertices = function(data, offsetter, side) {
    var vertices = [];
    vertices.side = side;
    vertices.mode = 'sampled';
    vertices.offsetter = offsetter;
    vertices.vertexSetId = side.vertexSet.id;

    var sizeAttr = (side.vertexAttrs.size) ? side.vertexAttrs.size.id : undefined;
    var sizeMax = 1;
    if (sizeAttr) {
      var vertexSizeBounds = common.minmax(mapByAttr(data.vertices, sizeAttr, 'double'));
      sizeMax = vertexSizeBounds.max;
    }

    var labelSizeAttr = (side.vertexAttrs.labelSize) ? side.vertexAttrs.labelSize.id : undefined;
    var labelSizeMax = 1;
    if (labelSizeAttr) {
      var labelSizeBounds = common.minmax(mapByAttr(data.vertices, labelSizeAttr, 'double'));
      labelSizeMax = labelSizeBounds.max;
    }

    var s = (offsetter.xOff < this.svg.width() / 2) ? 'left' : 'right';

    var colorAttr = (side.vertexAttrs.color) ? side.vertexAttrs.color.id : undefined;
    var colorMap = this.setupColorMap(data.vertices, side.vertexAttrs.color, s, 'Vertex Color');

    var labelColorAttr = (side.vertexAttrs.labelColor) ? side.vertexAttrs.labelColor.id : undefined;
    var labelColorMap = this.setupColorMap(
      data.vertices, side.vertexAttrs.labelColor, s, 'Label Color');

    var opacityAttr = (side.vertexAttrs.opacity) ? side.vertexAttrs.opacity.id : undefined;
    var opacityMax = 1;
    if (opacityAttr) {
      var opacityBounds = common.minmax(mapByAttr(data.vertices, opacityAttr, 'double'));
      opacityMax = opacityBounds.max;
    }

    for (var i = 0; i < data.vertices.length; ++i) {
      var vertex = data.vertices[i];

      var label;
      if (side.vertexAttrs.label) { label = vertex.attrs[side.vertexAttrs.label.id].string; }

      var size = 0.5;
      if (sizeAttr) { size = vertex.attrs[sizeAttr].double / sizeMax; }

      var labelSize = 0.5;
      if (labelSizeAttr) { labelSize = vertex.attrs[labelSizeAttr].double / labelSizeMax; }

      var color = UNCOLORED;
      if (colorAttr && vertex.attrs[colorAttr].defined) {
        // in case of doubles the keys are strings converted from the DynamicValue's double field
        // we can't just use the string field of the DynamicValue as 1.0 would turn to '1'
        color = (side.vertexAttrs.color.typeName === 'Double') ?
          colorMap[vertex.attrs[colorAttr].double] : colorMap[vertex.attrs[colorAttr].string];
      }

      var labelColor;
      if (labelColorAttr && vertex.attrs[labelColorAttr].defined) {
        // in case of doubles the keys are strings converted from the DynamicValue's double field
        // we can't just use the string field of the DynamicValue as 1.0 would turn to '1'
        labelColor = (side.vertexAttrs.labelColor.typeName === 'Double') ?
          labelColorMap[vertex.attrs[labelColorAttr].double] :
          labelColorMap[vertex.attrs[labelColorAttr].string];
      }

      var opacity = 1;
      if (opacityAttr) { opacity = vertex.attrs[opacityAttr].double / opacityMax; }

      var icon;
      if (side.vertexAttrs.icon) { icon = vertex.attrs[side.vertexAttrs.icon.id].string; }
      var image;
      if (side.vertexAttrs.image) { image = vertex.attrs[side.vertexAttrs.image.id].string; }

      var radius = 0.1 * Math.sqrt(size);
      var v = new Vertex(vertex,
                         Math.random() * 400 - 200,
                         Math.random() * 400 - 200,
                         radius,
                         label,
                         labelSize,
                         color,
                         opacity,
                         labelColor,
                         icon,
                         image);
      offsetter.rule(v);
      v.id = vertex.id.toString();
      svg.addClass(v.dom, 'sampled');
      if (side.centers.indexOf(v.id) > -1) {
        svg.addClass(v.dom, 'center');
      }
      vertices.push(v);

      this.sampledVertexMouseBindings(vertices, v);
      this.vertexGroup.append(v.dom);
    }

    return vertices;
  };

  GraphView.prototype.addColorLegend = function (colorMap, side, title) {
    var margin = 50;
    var x = side === 'left' ? margin : this.svg.width() - margin;
    var anchor = side === 'left' ? 'start' : 'end';
    var i = this.legendNextLine;
    var titleSvg = svg.create('text', { 'class': 'legend', x: x, y: i * 22 + margin }) .text(title);
    this.legend.append(titleSvg);
    i++;
    for (var attr in colorMap) {
      var l = svg.create('text', { 'class': 'legend', x: x, y: i * 22 + margin })
        .text(attr || 'undefined');
      l.attr('style', 'fill: ' + colorMap[attr] || UNCOLORED);
      l.attr('text-anchor', anchor);
      this.legend.append(l);
      i++;
    }
    this.legendNextLine = i;
  };

  function translateTouchToMouseEvent(ev) {
    if (ev.type === 'touchmove') {
      // Translate into mouse event.
      ev.pageX = ev.originalEvent.changedTouches[0].pageX;
      ev.pageY = ev.originalEvent.changedTouches[0].pageY;
      ev.preventDefault();
    }
  }

  GraphView.prototype.sampledVertexMouseBindings = function(vertices, vertex) {
    var scope = this.scope;
    var svgElement = this.svg;
    var vertexGroup = this.vertexGroup;
    vertex.dom.on('mousedown touchstart', function(evStart) {
      evStart.stopPropagation();
      vertex.hold();
      vertex.dragged = false;
      vertexGroup.append(vertex.dom);  // Bring to top.
      angular.element(window).on('mouseup touchend', function() {
        angular.element(window).off('mousemove mouseup touchmove touchend');
        if (!vertex.held) {
          return;  // Duplicate event.
        }
        vertex.release();
        if (vertex.dragged) {  // It was a drag.
          vertex.dragged = false;
          vertices.animate();
        } else {  // It was a click.
          scope.$apply(function() {
            var actions = [];
            var side = vertices.side;
            var id = vertex.id.toString();
            if (!side.hasCenter(id)) {
              actions.push({
                title: 'Add to centers',
                callback: function() {
                  side.addCenter(id);
                },
              });
            }
            if (side.hasCenter(id)) {
              actions.push({
                title: 'Remove from centers',
                callback: function() {
                  side.removeCenter(id);
                },
              });
            }
            if (!side.hasCenter(id) || (side.centers.length !== 1)) {
              actions.push({
                title: 'Set as only center',
                callback: function() {
                  side.setCenter(id);
                },
              });
            }
            if (side.hasParent()) {
              if (side.isParentFilteredToSegment(id)) {
                actions.push({
                  title: 'Stop filtering base project to this segment',
                  callback: function() {
                    side.deleteParentsSegmentFilter();
                  },
                });
              } else {
                actions.push({
                  title: 'Filter base project to this segment',
                  callback: function() {
                    side.filterParentToSegment(id);
                  },
                });
              }
            }
            if (side.hasSegmentation()) {
              if (side.isSegmentationFilteredToParent(id)) {
                actions.push({
                  title: 'Stop filtering segmentation to this vertex',
                  callback: function() {
                    side.deleteSegmentationsParentFilter();
                  },
                });
              } else {
                actions.push({
                  title: 'Filter segmentation to this vertex',
                  callback: function() {
                    side.filterSegmentationToParent(id);
                  },
                });
              }
            }
            if (vertex.frozen) {
              actions.push({
                title: 'Unfreeze',
                callback: function() {
                  vertex.frozen -= 1;
                  vertices.animate();
                },
              });
            } else {
              actions.push({
                title: 'Freeze',
                callback: function() {
                  vertex.frozen += 1;
                },
              });
            }

            vertex.activateMenu({
              header: 'Vertex ' + id,
              type: 'vertex',
              id: id,
              actions: actions,
            });
          });
        }
      });
      angular.element(window).on('mousemove touchmove', function(ev) {
        if (vertex.positioned) { return; }
        translateTouchToMouseEvent(ev);
        var offsetter = vertex.offsetter;
        var x = (ev.pageX - svgElement.offset().left - offsetter.xOff) / offsetter.zoom;
        var y = (ev.pageY - svgElement.offset().top - offsetter.yOff) / offsetter.zoom;
        vertex.moveTo(x, y);
        vertex.forceOX = x;
        vertex.forceOY = y;
        vertex.dragged = true;
        vertices.animate();
      });
    });
  };

  GraphView.prototype.bucketedVertexMouseBindings = function(vertices, vertex) {
    var scope = this.scope;
    vertex.dom.click(function() {
      scope.$apply(function() {
        var actions = [];
        var side = vertices.side;
        var xAttr = vertices.xAttribute;
        var yAttr = vertices.yAttribute;
        if (xAttr && vertex.xFilter) {
          actions.push({
            title: 'Add filter for ' + xAttr.title,
            callback: function() {
              side.setVertexFilter(xAttr.title, vertex.xFilter);
            },
          });
        }
        if (yAttr && vertex.yFilter) {
          actions.push({
            title: 'Add filter for ' + yAttr.title,
            callback: function() {
              side.setVertexFilter(yAttr.title, vertex.yFilter);
            },
          });
        }
        if (xAttr && yAttr && vertex.xFilter && vertex.yFilter) {
          actions.push({
            title: 'Add filter for ' + xAttr.title + ' & ' + yAttr.title,
            callback: function() {
              side.setVertexFilter(xAttr.title, vertex.xFilter);
              side.setVertexFilter(yAttr.title, vertex.yFilter);
            },
          });
        }
        if (actions.length > 0) {
          vertex.activateMenu({
            actions: actions,
          });
        }
      });
    });
  };

  GraphView.prototype.sideMouseBindings = function(offsetter, xMin, xMax) {
    var svgElement = this.svg;
    this.svgMouseDownListeners.push(function(evStart) {
      translateTouchToMouseEvent(evStart);
      var svgX = evStart.pageX - svgElement.offset().left;
      if ((svgX < xMin) || (svgX >= xMax)) {
        return;
      }
      var evXToXOff = offsetter.xOff - evStart.pageX;
      var evYToYOff = offsetter.yOff - evStart.pageY;
      angular.element(window).on('mousemove touchmove', function(evMoved) {
        translateTouchToMouseEvent(evMoved);
        offsetter.panTo(evMoved.pageX + evXToXOff, evMoved.pageY + evYToYOff);
      });
      angular.element(window).on('mouseup touchend', function() {
        angular.element(window).off('mousemove mouseup touchmove touchend');
      });
    });
    function zoom(position, deltaZoom, deltaThickness) {
      var delta = -0.001 * deltaZoom;
      // Graph-space point under the mouse should remain unchanged.
      // mxOff * zoom + xOff = position.x
      var mxOff = (position.x - offsetter.xOff) / offsetter.zoom;
      var myOff = (position.y - offsetter.yOff) / offsetter.zoom;
      offsetter.zoom *= Math.exp(delta);
      offsetter.xOff = position.x - mxOff * offsetter.zoom;
      offsetter.yOff = position.y - myOff * offsetter.zoom;
      // Shift-scroll, or horizontal scroll is applied only to thickness.
      delta += -0.005 * deltaThickness;
      // Thickness (vertex radius and edge width) changes by a square-root function.
      offsetter.thickness *= Math.exp(0.5 * delta);
      offsetter.reDraw();
    }
    this.svgMouseWheelListeners.push(function(e) {
      var mx = e.originalEvent.pageX;
      var my = e.originalEvent.pageY;
      var svgX = mx - svgElement.offset().left;
      if ((svgX < xMin) || (svgX >= xMax)) {
        return;
      }
      e.preventDefault();
      var oe = e.originalEvent;
      var plainScroll = oe.shiftKey ? 0 : oe.deltaY;
      var shiftScroll = oe.deltaX + (oe.shiftKey ? oe.deltaY : 0);
      zoom({ x: mx, y: my }, plainScroll, shiftScroll);
    });
    this.svgDoubleClickListeners.push(function(e) {
      var mx = e.originalEvent.pageX;
      var my = e.originalEvent.pageY;
      var svgX = mx - svgElement.offset().left;
      if ((svgX < xMin) || (svgX >= xMax)) {
        return;
      }
      e.preventDefault();
      // Left/right is in/out.
      var scroll = e.which === 1 ? -500 : 500;
      // Shift affects thickness.
      var shift = e.originalEvent.shiftKey;
      zoom(
        { x: mx, y: my },
        shift ? 0 : scroll,
        shift ? 0.5 * scroll : 0);
    });
  };

  GraphView.prototype.initSampled = function(vertices) {
    this.initLayout(vertices);
    this.initView(vertices);
    this.initSlider(vertices);
  };

  // Returns the ideal zoom setting for the given coordinate bounds,
  // or undefined if one cannot be found.
  function zoomFor(xb, yb, width, height) {
    var xCenter = (xb.min + xb.max) / 2;
    var yCenter = (yb.min + yb.max) / 2;
    var xFit = 0.5 * width / (xb.max - xCenter);
    var yFit = 0.5 * height / (yb.max - yCenter);
    // Avoid infinite zoom for 1-vertex graphs.
    if (isFinite(xFit) || isFinite(yFit)) {
      // Take absolute value, just in case we have negative infinity.
      return graphToSVGRatio * Math.min(Math.abs(xFit), Math.abs(yFit));
    }
    return undefined;
  }

  // Returns the ideal panning coordinates. xb and yb are the coordinate bounds,
  // zoom is the offsetter zoom setting, width and height are the viewport dimensions,
  // and xMin is the viewport X offset.
  function panFor(xb, yb, zoom, width, height, xMin) {
    // The bounds of panning positions that can see the graph.
    var xOffMin = -xb.max * zoom + xMin;
    var xOffMax = -xb.min * zoom + xMin + width;
    var yOffMin = -yb.max * zoom;
    var yOffMax = -yb.min * zoom + height;
    return {
      // Returns true if the given offset is also acceptable.
      acceptable: function(xOff, yOff) {
        return xOffMin <= xOff && xOff <= xOffMax && yOffMin <= yOff && yOff <= yOffMax;
      },
      xOff: (xOffMin + xOffMax) / 2,
      yOff: (yOffMin + yOffMax) / 2,
    };
  }

  // Pan/zoom the view (the offsetter) to fit the graph, if necessary.
  GraphView.prototype.initView = function(vertices) {
    var offsetter = vertices.offsetter;
    // Figure out zoom.
    var xb = common.minmax(vertices.map(function(v) { return v.x; }));
    var yb = common.minmax(vertices.map(function(v) { return v.y; }));
    var width = vertices.halfColumnWidth * 2;
    var height = this.svg.height();
    var newZoom = zoomFor(xb, yb, width, height) || offsetter.zoom;
    var newPan = panFor(xb, yb, offsetter.zoom, width, height, vertices.xMin);
    // Apply the calculated zoom if it is a new offsetter, or if the inherited zoom is way off.
    var ratio = newZoom / offsetter.zoom;
    if (!offsetter.inherited ||
        ratio < 0.1 || ratio > 10 ||
        !newPan.acceptable(offsetter.xOff, offsetter.yOff)) {
      offsetter.zoom = newZoom;
      // Recalculate with the new zoom.
      newPan = panFor(xb, yb, newZoom, width, height, vertices.xMin);
      offsetter.xOff = newPan.xOff;
      offsetter.yOff = newPan.yOff;
    }
    offsetter.reDraw();
  };

  GraphView.prototype.initSlider = function(vertices) {
    this.unregistration.push(this.scope.$watch(sliderPos, onSlider));
    function sliderPos() {
      return vertices.side.sliderPos;
    }
    function onSlider() {
      var sliderAttr = vertices.side.vertexAttrs.slider;
      if (!sliderAttr) { return; }
      var sb = common.minmax(
          vertices.map(function(v) { return v.data.attrs[sliderAttr.id].double; }));
      var pos = Number(sliderPos());
      for (var i = 0; i < vertices.length; ++i) {
        var v = vertices[i];
        var x =
          v.data.attrs[sliderAttr.id].defined ? v.data.attrs[sliderAttr.id].double : undefined;
        var norm = Math.floor(100 * common.normalize(x, sb) + 50);  // Normalize to 0 - 100.
        if (norm < pos) {
          v.color = 'hsl(120, 50%, 42%)';
        } else if (norm > pos) {
          v.color = 'hsl(0, 50%, 42%)';
        } else if (norm === pos) {
          v.color = 'hsl(60, 60%, 45%)';
        } else {
          v.color = 'hsl(60, 0%, 42%)';
        }
        v.icon.attr({ style: 'fill: ' + v.color });
      }
    }
  };

  function Clipper(bounds) {
    var rnd = Math.random().toString(36);
    var defs = svg.create('defs');
    var clip = svg.create('clipPath', { id: 'clip-' + rnd });
    var rect = svg.create('rect', bounds);
    clip.append(rect);
    defs.append(clip);
    this.dom = defs;
    this.url = 'url(#clip-' + rnd + ')';
  }

  function Map(gv, vertices) {
    this.gv = gv;
    this.vertices = vertices;
    var clipper = new Clipper({
      x: vertices.xMin,
      y: 0,
      width: vertices.halfColumnWidth * 2,
      height: gv.svg.height() });
    this.gv.root.prepend(clipper.dom);
    this.group = svg.create('g', { 'class': 'map', 'clip-path': clipper.url });
    this.gv.root.prepend(this.group);
    // The size of the Earth in lat/long view. It doesn't make much difference,
    // just has to be a reasonable value to avoid too small/too large numbers.
    this.GLOBE_SIZE = 500;
    // Constant to match Google Maps projection.
    this.GM_MULT = 0.403;
    // How much to wait after pan/zoom events before requesting a new map.
    this.NAVIGATION_DELAY = 100;  // Milliseconds.
    this.root = 'https://maps.googleapis.com/maps/api/staticmap?';
    this.style = 'feature:all|gamma:0.1|saturation:-80';
    this.key = 'AIzaSyBcML5zQetjkRFuqpSSG6EmhS2vSWRssZ4';  // big-graph-gc1 API key.
    this.images = [];
    this.vertices.offsetter.rule(this);
  }
  Map.prototype.lon2x = function(lon) {
    return this.GLOBE_SIZE * lon / 360;
  };
  Map.prototype.lat2y = function(lat) {
    return -this.GLOBE_SIZE * Math.log(
        Math.tan(lat * Math.PI / 180) +
        1 / Math.cos(lat * Math.PI / 180)
        ) / Math.PI / 2;
  };
  Map.prototype.x2lon = function(x) {
    return x * 360 / this.GLOBE_SIZE;
  };
  Map.prototype.y2lat = function(y) {
    return -Math.atan(Math.sinh(y * Math.PI * 2 / this.GLOBE_SIZE)) * 180 / Math.PI;
  };
  Map.prototype.reDraw = function() {
    var offsetter = this.offsetter;
    for (var i = 0; i < this.images.length; ++i) {
      var image = this.images[i];
      image.attr({
        x: image.x * offsetter.zoom + offsetter.xOff,
        y: image.y * offsetter.zoom + offsetter.yOff,
        width: offsetter.zoom * image.size,
        height: offsetter.zoom * image.size,
      });
    }
    if (this.lastZoom !== offsetter.zoom ||
        this.lastXOff !== offsetter.xOff ||
        this.lastYOff !== offsetter.yOff) {
      this.lastZoom = offsetter.zoom;
      this.lastXOff = offsetter.xOff;
      this.lastYOff = offsetter.yOff;
      $timeout.cancel(this.refresh);
      var that = this;
      this.refresh = $timeout(function() { that.update(); }, this.NAVIGATION_DELAY);
    }
  };
  Map.prototype.update = function() {
    var w = this.vertices.halfColumnWidth * 2;
    var h = this.gv.svg.height();
    var offsetter = this.offsetter;
    var x = (w / 2 - offsetter.xOff + this.vertices.xMin) / offsetter.zoom;
    var y = (h / 2 - offsetter.yOff) / offsetter.zoom;
    var zoomLevel = Math.log2(this.GLOBE_SIZE * offsetter.zoom / Math.max(w, h) / this.GM_MULT);
    zoomLevel = Math.max(0, Math.floor(zoomLevel));
    var clat = this.y2lat(y);
    var clon = this.x2lon(x);
    var image = svg.create('image');
    var href = (
      this.root + 'center=' + clat + ',' + clon + '&zoom=' + zoomLevel +
      '&key=' + this.key +
      '&size=640x640&scale=2&style=' + this.style);
    image[0].setAttributeNS('http://www.w3.org/1999/xlink', 'href', href);
    image.size = this.GLOBE_SIZE * Math.pow(2, -zoomLevel) / this.GM_MULT;
    image.x = x - image.size / 2;
    image.y = y - image.size / 2;
    this.group.append(image);
    var images = this.images;
    images.push(image);
    // Discard old images, keeping the two most recent.
    for (var i = 0; i < images.length - 2; ++i) {
      images[i].remove();
    }
    images.splice(0, images.length - 2);
    this.reDraw();
  };

  GraphView.prototype.initLayout = function(vertices) {
    var positionAttr =
      (vertices.side.vertexAttrs.position) ? vertices.side.vertexAttrs.position.id : undefined;
    var geoAttr =
      (vertices.side.vertexAttrs.geo) ? vertices.side.vertexAttrs.geo.id : undefined;
    var map;
    if (geoAttr !== undefined) {
      map = new Map(this, vertices);
    }
    for (var i = 0; i < vertices.length; ++i) {
      var v = vertices[i];
      v.forceMass = 1;
      var pos;
      if (positionAttr !== undefined && v.data.attrs[positionAttr].defined) {
        pos = v.data.attrs[positionAttr];
        v.x = pos.x;
        v.y = -pos.y;  // Flip Y axis to look more mathematical.
        v.setPositioned();
      }
      if (geoAttr !== undefined && v.data.attrs[geoAttr].defined) {
        pos = v.data.attrs[geoAttr];
        v.x = map.lon2x(pos.y);
        v.y = map.lat2y(pos.x);
        v.setPositioned();
      }
      v.forceOX = v.x;
      v.forceOY = v.y;
    }
    if (vertices.edges !== undefined) {
      for (i = 0; i < vertices.edges.length; ++i) {
        var e = vertices.edges[i];
        e.src.forceMass += 1;
        e.dst.forceMass += 1;
      }
    }
    var scale = this.svg.height();
    var engine = new FORCE_LAYOUT.Engine({
      attraction: 0.01,
      repulsion: scale,
      gravity: 0.05,
      drag: 0.2,
      labelAttraction: parseFloat(vertices.side.animate.labelAttraction),
    });
    // Generate initial layout for 2 seconds or until it stabilizes.
    var t1 = Date.now();
    while (engine.calculate(vertices) && Date.now() - t1 <= 2000) {}
    engine.apply(vertices);
    var animating = false;
    // Call vertices.animate() later to trigger interactive layout.
    vertices.animate = function() {
      if (!animating) {
        animating = true;
        window.requestAnimationFrame(vertices.step);
      }
    };
    vertices.step = function() {
      engine.opts.labelAttraction = parseFloat(vertices.side.animate.labelAttraction);
      if (animating && vertices.side.animate.enabled && engine.step(vertices)) {
        window.requestAnimationFrame(vertices.step);
      } else {
        animating = false;
      }
    };
    vertices.animate();
    // Kick off animation when the user manually turns it on.
    var unwatch = util.deepWatch(this.scope,
        function() { return vertices.side.animate; },
        function() { vertices.animate(); });
    this.unregistration.push(function() {
      unwatch();
      animating = false;
    });
  };

  GraphView.prototype.addBucketedVertices = function(data, offsetter, viewData) {
    var vertices = [];
    vertices.side = viewData;
    vertices.mode = 'bucketed';
    vertices.offsetter = offsetter;
    vertices.xAttribute = viewData.xAttribute;
    vertices.yAttribute = viewData.yAttribute;
    var xLabels = [], yLabels = [];
    var i, x, y, l, side;
    var labelSpace = 0.05;
    y = 0.5 + labelSpace;

    var xb = common.minmax(data.vertices.map(function(n) { return n.x; }));
    var yb = common.minmax(data.vertices.map(function(n) { return n.y; }));

    var xNumBuckets = xb.span + 1;
    var yNumBuckets = yb.span + 1;

    y = 0.5 + labelSpace;
    if (viewData.xAttribute) {
      // Label the X axis with the attribute name.
      l = new Label(
          0, y - labelSpace, viewData.xAttribute.title,
          { classes: 'axis-label' });
      offsetter.rule(l);
      this.vertexGroup.append(l.dom);
    }
    for (i = 0; i < data.xLabels.length; ++i) {
      if (data.xLabelType === 'between') {
        x = common.normalize(i, xNumBuckets);
      } else {
        x = common.normalize(i + 0.5, xNumBuckets);
      }
      l = new Label(x, y, data.xLabels[i]);
      offsetter.rule(l);
      xLabels.push(l);
      this.vertexGroup.append(l.dom);
    }
    // Labels on the left on the left and on the right on the right.
    if (offsetter.xOff < this.svg.width() / 2) {
      x = -0.5 - labelSpace;
      side = 'left';
    } else {
      x = 0.5 + labelSpace;
      side = 'right';
    }
    if (viewData.yAttribute) {
      // Label the Y axis with the attribute name.
      var mul = side === 'left' ? 1 : -1;
      l = new Label(
          x + mul * labelSpace, 0, viewData.yAttribute.title,
          { vertical: true, classes: 'axis-label' });
      offsetter.rule(l);
      this.vertexGroup.append(l.dom);
    }
    for (i = 0; i < data.yLabels.length; ++i) {
      if (data.yLabelType === 'between') {
        y = -common.normalize(i, yNumBuckets);
      } else {
        y = -common.normalize(i + 0.5, yNumBuckets);
      }
      l = new Label(x, y, data.yLabels[i], { classes: side });
      offsetter.rule(l);
      yLabels.push(l);
      this.vertexGroup.append(l.dom);
    }

    var sizes = data.vertices.map(function(n) { return n.size; });
    var vertexScale = 1 / common.minmax(sizes).max;
    for (i = 0; i < data.vertices.length; ++i) {
      var vertex = data.vertices[i];
      var radius = 0.1 * Math.sqrt(vertexScale * vertex.size);
      var v = new Vertex(vertex,
                         common.normalize(vertex.x + 0.5, xNumBuckets),
                         -common.normalize(vertex.y + 0.5, yNumBuckets),
                         radius,
                         vertex.size);
      offsetter.rule(v);
      vertices.push(v);
      if (vertex.size === 0) {
        continue;
      }
      this.bucketedVertexMouseBindings(vertices, v);
      this.vertexGroup.append(v.dom);
      if (xLabels.length !== 0) {
        v.addHoverListener(xLabels[vertex.x]);
        if (data.xLabelType === 'between') { v.addHoverListener(xLabels[vertex.x + 1]); }
      }
      if (data.xFilters.length > 0) {
        v.xFilter = data.xFilters[vertex.x];
      }
      if (yLabels.length !== 0) {
        v.addHoverListener(yLabels[vertex.y]);
        if (data.yLabelType === 'between') { v.addHoverListener(yLabels[vertex.y + 1]); }
      }
      if (data.yFilters.length > 0) {
        v.yFilter = data.yFilters[vertex.y];
      }
    }
    return vertices;
  };

  GraphView.prototype.addEdges = function(edges, srcs, dsts, side) {
    var widthKey;
    var colorKey;
    var colorMap;
    var labelKey;
    if (side) {
      var attrKey = function(aggrAttr) {
        if (aggrAttr) {
          return aggrAttr.id + ':' + aggrAttr.aggregator;
        }
        return undefined;
      };
      var sideString = (srcs.offsetter.xOff < this.svg.width() / 2) ? 'left' : 'right';
      widthKey = attrKey(side.edgeAttrs.width);
      colorKey = attrKey(side.edgeAttrs.edgeColor);
      colorMap = this.setupColorMap(
        edges, side.edgeAttrs.edgeColor, sideString, 'Edge Color', colorKey);
      labelKey = attrKey(side.edgeAttrs.edgeLabel);
    }

    var edgeObjects = [];
    var edgeWidths = edges.map(function(e) {
      if (widthKey) {
        return (e.attrs[widthKey] || { double: 0.0 }).double;
      }
      return e.size;
    });
    var bounds = common.minmax(edgeWidths);
    var normalWidth = 0.02;
    var info = bounds.span / bounds.max;  // Information content of edge widths. (0 to 1)
    // Go up to 3x thicker lines if they are meaningful.
    var edgeScale = normalWidth * (1 + info * 2) / bounds.max;
    for (var i = 0; i < edges.length; ++i) {
      var edge = edges[i];
      var width = edgeWidths[i];
      if (edge.size === 0) {
        continue;
      }
      var a = srcs[edge.a];
      var b = dsts[edge.b];

      var color;
      if (colorKey && edge.attrs[colorKey].defined) {
        color = colorMap[edge.attrs[colorKey].double];
      }
      var label;
      if (labelKey) {
        label = edge.attrs[labelKey].string;
      }
      var e = new Edge(a, b, edgeScale * width, color, label);
      edgeObjects.push(e);
      this.edgeGroup.append(e.dom);
    }
    return edgeObjects;
  };

  function Label(x, y, text, opts) {
    opts = opts || {};
    var classes = 'bucket ' + (opts.classes || '');
    this.x = x;
    this.y = y;
    this.vertical = opts.vertical;
    this.dom = svg.create('text', { 'class': classes }).text(text);
    if (this.vertical) {
      this.dom.attr({ transform: svgRotate(-90) });
    }
  }
  Label.prototype.on = function() { svg.addClass(this.dom, 'highlight'); };
  Label.prototype.off = function() { svg.removeClass(this.dom, 'highlight'); };
  Label.prototype.reDraw = function() {
    if (this.vertical) {
      this.dom.attr({x: -this.screenY(), y: this.screenX()});
    } else {
      this.dom.attr({x: this.screenX(), y: this.screenY()});
    }
  };

  function Vertex(data, x, y, r, text, textSize, color, opacity, labelColor, icon, image) {
    this.data = data;
    this.x = x;
    this.y = y;
    this.r = r;
    this.color = color || UNCOLORED;
    if (this.color === UNCOLORED) {
      this.highlight = 'white';
    } else {
      this.highlight = tinycolor(this.color).lighten(20).toString();
    }
    this.frozen = 0;  // Number of reasons why this vertex should not be animated.
    this.positioned = false;  // Is this vertex explicitly positioned?
    if (image) {
      this.icon = svg.create('image', { width: 1, height: 1 });
      this.icon[0].setAttributeNS('http://www.w3.org/1999/xlink', 'href', image);
      this.icon.center = { x: 0.5, y: 0.5 };
      this.icon.scale = 2.0;
    } else {
      this.icon = getIcon(icon);
      this.icon.attr({ style: 'fill: ' + this.color, 'class': 'icon' });
    }
    this.minTouchRadius = 10;
    if (r < this.minTouchRadius) {
      this.touch = svg.create('circle', { 'class': 'touch' });
    } else {
      this.touch = this.icon;
    }
    this.text = text;
    var fontSize = 30 * textSize;
    this.label = svg.create('text', { 'font-size': fontSize + 'px' }).text(text || '');
    if (labelColor !== undefined) {
      this.label.attr({ style: 'fill: ' + labelColor });
    }
    this.labelBackground = svg.create(
        'rect', {
          'class': 'label-background',
          width: 0, height: 0, rx: 2, ry: 2,
        });
    this.dom = svg.group(
        [this.icon, this.touch, this.labelBackground, this.label],
        {'class': 'vertex' });
    this.dom.attr({ opacity: opacity });
    this.moveListeners = [];
    this.highlightListeners = [];
    this.hoverListeners = [];
    // Notified when this vertex becomes opaque.
    this.opaqueListeners = [];
    this.isOpaque = false;
    var that = this;
    this.touch.mouseenter(function() {
      // Put the "fade-non-opaque" class on the whole SVG.
      svg.addClass(that.dom.closest('svg'), 'fade-non-opaque');
      if (!that.positioned) {
        that.setHighlight(true);
      }
      for (var i = 0; i < that.hoverListeners.length; ++i) {
        that.hoverListeners[i].on(that);
      }
    });
    this.touch.mouseleave(function() {
      // Remove the "fade-non-opaque" class from the whole SVG.
      svg.removeClass(that.dom.closest('svg'), 'fade-non-opaque');
      if (!that.held && !that.positioned) {
        that.setHighlight(false);
      }
      for (var i = 0; i < that.hoverListeners.length; ++i) {
        that.hoverListeners[i].off(that);
      }
    });
  }

  Vertex.prototype.setHighlight = function(on) {
    var i;
    if (on) {
      svg.addClass(this.dom, 'highlight');
      this.icon.attr({style: 'fill: ' + this.highlight});
      for (i = 0; i < this.highlightListeners.length; ++i) {
        this.highlightListeners[i].on(this);
      }
      // Size labelBackground here, because we may not know the label size earlier.
      this.labelBackground.attr({
        width: this.label.width() + 4,
        height: this.label.height(),
        style: 'fill: ' + this.highlight,
      });
      this.reDraw();
    } else {
      svg.removeClass(this.dom, 'highlight');
      this.icon.attr({style: 'fill: ' + this.color});
      for (i = 0; i < this.highlightListeners.length; ++i) {
        this.highlightListeners[i].off(this);
      }
      this.labelBackground.attr({ style: '' });
    }
  };

  // Hover and highlight listeners must have an `on()` and an `off()` method.
  // "Hover" is used for the one vertex under the cursor.
  // "Highlight" is used for any vertices that we want to render more visible.
  Vertex.prototype.addHoverListener = function(hl) {
    this.hoverListeners.push(hl);
  };
  Vertex.prototype.addHighlightListener = function(hl) {
    this.highlightListeners.push(hl);
  };

  Vertex.prototype.addOpaqueListener = function(ol) {
    this.opaqueListeners.push(ol);
  };
  Vertex.prototype.setOpaque = function(on) {
    this.isOpaque = on;
    if (on) {
      svg.addClass(this.dom, 'opaque');
    } else {
      svg.removeClass(this.dom, 'opaque');
    }
    for (var i = 0; i < this.opaqueListeners.length; ++i) {
      this.opaqueListeners[i]();
    }
  };

  // Mark this vertex as explicitly positioned (as on a map).
  Vertex.prototype.setPositioned = function() {
    if (this.positioned) { return; }
    this.positioned = true;
    // Positioned vertices are highlighted to increase the contrast against the map,
    // and to distinguish them.
    this.setHighlight(true);
  };

  Vertex.prototype.addMoveListener = function(ml) {
    this.moveListeners.push(ml);
  };
  Vertex.prototype.moveTo = function(x, y) {
    this.x = x;
    this.y = y;
    this.reDraw();
  };
  Vertex.prototype.hold = function() {
    this.held = true;
  };
  Vertex.prototype.release = function() {
    this.held = false;
    this.touch.mouseleave();
  };
  function svgTranslate(x, y) { return ' translate(' + x + ' ' + y + ')'; }
  function svgScale(s) { return ' scale(' + s + ')'; }
  function svgRotate(deg) { return ' rotate(' + deg + ')'; }
  Vertex.prototype.reDraw = function() {
    var sx = this.screenX(), sy = this.screenY();
    var r = this.offsetter.thickness * this.r;
    this.icon.attr({ transform:
      svgTranslate(sx, sy) +
      svgScale(r * this.icon.scale) +
      svgTranslate(-this.icon.center.x, -this.icon.center.y) });
    this.touch.attr({ cx: sx, cy: sy, r: r });
    this.label.attr({ x: sx, y: sy });
    var backgroundWidth = this.labelBackground.attr('width');
    var backgroundHeight = this.labelBackground.attr('height');
    this.labelBackground.attr({ x: sx - backgroundWidth / 2, y: sy - backgroundHeight / 2 });
    for (var i = 0; i < this.moveListeners.length; ++i) {
      this.moveListeners[i](this);
    }
  };

  function Edge(src, dst, w, color, label) {
    this.src = src;
    this.dst = dst;
    this.w = w;
    this.first = svg.create('path', { 'class': 'first' });
    this.second = svg.create('path', { 'class': 'second' });
    if (color) {
      this.first.attr({ style: 'stroke: ' + color });
      this.second.attr({ style: 'stroke: ' + color });
    }
    var fontSize = 15;
    this.label = svg.create('text', { 'font-size': fontSize + 'px' }).text(label || '');
    this.dom = svg.group([this.second, this.first, this.label], {'class': 'edge'});
    var that = this;
    src.addMoveListener(function() { that.reposition(); });
    dst.addMoveListener(function() { that.reposition(); });
    this.reposition();
    // To highlight the neighborhood of a hovered/highlighted vertex, the
    // following rules are implemented below:
    //  - On either end's hover the edge sets both of its endpoints opaque.
    //  - On both ends becoming opaque the edge makes itself opaque.
    //  - On either end's highlight the edge highlights itself.
    function highlightListener(cls) {
      return {
        on: function() {
          svg.addClass(that.dom, cls);
          that.toFront();
        },
        off: function() {
          svg.removeClass(that.dom, cls);
        },
      };
    }
    var hoverListener = {
      on: function() {
        src.setOpaque(true);
        dst.setOpaque(true);
      },
      off: function() {
        src.setOpaque(false);
        dst.setOpaque(false);
      },
    };
    src.addHighlightListener(highlightListener('highlight-out'));
    src.addHoverListener(hoverListener);
    if (src !== dst) {
      dst.addHighlightListener(highlightListener('highlight-in'));
      dst.addHoverListener(hoverListener);
    }
    var opaqueListener = function() {
      if (src.isOpaque && dst.isOpaque) {
        svg.addClass(that.dom, 'opaque');
      } else {
        svg.removeClass(that.dom, 'opaque');
      }
    };
    src.addOpaqueListener(opaqueListener);
    dst.addOpaqueListener(opaqueListener);
  }
  Edge.prototype.toFront = function() {
    this.dom.parent().append(this.dom);
  };
  Edge.prototype.reposition = function() {
    var src = this.src, dst = this.dst;
    var avgZoom = 0.5 * (src.offsetter.thickness + dst.offsetter.thickness);
    this.first.attr({
      d: svg.arrow1(
        src.screenX(), src.screenY(), dst.screenX(), dst.screenY(), avgZoom),
      'stroke-width': avgZoom * this.w,
    });
    this.second.attr({
      d: svg.arrow2(
        src.screenX(), src.screenY(), dst.screenX(), dst.screenY(), avgZoom),
      'stroke-width': avgZoom * this.w,
    });
    var arcParams = svg.arcParams(
      src.screenX(), src.screenY(), dst.screenX(), dst.screenY(), avgZoom);
    this.label.attr({
      x: arcParams.x,
      y: arcParams.y,
    });
  };

  return directive;
});
