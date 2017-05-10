// Graph visualization. Generates the SVG contents.
'use strict';

angular.module('biggraph').directive('graphView', function(util, $compile, $timeout, $window) {
  /* global SVG_UTIL, COMMON_UTIL, FORCE_LAYOUT, tinycolor */
  var svg = SVG_UTIL;
  var common = COMMON_UTIL;
  var directive = {
    restrict: 'E',
    templateUrl: 'scripts/project/graph-view.html',
    scope: { graph: '=', menu: '=' },
    replace: true,
    link: function(scope, element) {
      element = angular.element(element);
      scope.gv = new GraphView(scope, element);
      scope.updateGraph = function() {
        if (scope.graph.view === undefined ||
          !scope.graph.view.$resolved ||
          !scope.gv.iconsLoaded()) {
          scope.gv.loading();
        } else if (scope.graph.view.$error) {
          scope.gv.error(scope.graph.view);
        } else {
          scope.gv.update(scope.graph.view, scope.menu);
        }
      };
      scope.$watch('graph.view', scope.updateGraph);
      scope.$watch('graph.view.$resolved', scope.updateGraph);
      // An attribute change can happen without a graph data change. Watch them separately.
      // (When switching from "color" to "slider", for example.)
      util.deepWatch(scope, 'graph.left.vertexAttrs', scope.updateGraph);
      util.deepWatch(scope, 'graph.right.vertexAttrs', scope.updateGraph);
      util.deepWatch(scope, 'graph.left.edgeAttrs', scope.updateGraph);
      util.deepWatch(scope, 'graph.right.edgeAttrs', scope.updateGraph);
      handleResizeEvents(scope);
    },
  };

  function handleResizeEvents(scope) {
    var timer;
    function update() {
      $timeout.cancel(timer);
      timer = $timeout(scope.updateGraph, 100);
    }

    var eventName = 'resize.graph-view-' + scope.$id;
    var window = angular.element($window);
    window.bind(eventName, update);
    scope.$on('ui.layout.toggle', update);
    scope.$on('ui.layout.resize', update);
    scope.$on('$destroy', function() {
      $timeout.cancel(timer);
      window.unbind(eventName);
    });
  }

  function Offsetter(xOff, yOff, zoom, thickness, menu, xMin, xMax, side) {
    this.xOff = xOff;
    this.yOff = yOff;
    this.zoom = zoom;  // Zoom for positions.
    this.thickness = thickness;  // Zoom for radius/width.
    this.menu = menu;
    this.elements = [];
    this.xMin = xMin;
    this.xMax = xMax;
    this.side = side;
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
    if (!this.drawing) {
      this.drawing = true;
      var that = this;
      // Call the actual drawing asynchronously.
      $timeout(function() {
        for (var i = 0; i < that.elements.length; ++i) {
          that.elements[i].reDraw();
        }
        that.drawing = false;
      });
    }
  };
  Offsetter.prototype.inherit = function() {
    var offsetter = new Offsetter(
        this.xOff, this.yOff, this.zoom, this.thickness, this.menu,
        this.xMin, this.xMax, this.side);
    offsetter.inherited = true;
    return offsetter;
  };

  function GraphView(scope, element) {
    this.scope = scope;
    this.unregistration = [];  // Cleanup functions to be called before building a new graph.
    this.rootElement = element;
    this.svg = element.find('svg.graph-view');
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

  GraphView.prototype.iconsLoaded = function() {
    return this.hasIcon('circle');
  };

  // Returns a reference to the icon inside #svg-icons.
  GraphView.prototype.getOriginalIcon = function(name) {
    return this.rootElement.find('#svg-icons #' + name.toLowerCase());
  };

  GraphView.prototype.hasIcon = function(name) {
    if (!name) { return false; }
    var icon = this.getOriginalIcon(name);
    return icon.length !== 0;
  };

  // Creates a scaled clone of the icon inside #svg-icons.
  GraphView.prototype.getIcon = function(name) {
    var icon = this.getOriginalIcon(name);
    var circle = this.getOriginalIcon('circle');
    var cbb = circle[0].getBBox();
    var bb = icon[0].getBBox();
    var clone = icon.clone();
    // Take the scaling factor from the circle icon.
    clone.scale = 2 / Math.max(cbb.width, cbb.height);
    clone.center = {
      x: bb.x + bb.width / 2,
      y: bb.y + bb.height / 2,
    };
    return clone;
  };

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
    var errorMessage = svg.create('text',
      {'class': 'error-message', x: x, y: y + 30, 'text-anchor': 'middle'});
    errorMessage.text(view.$error);
    this.root.append(errorMessage);
  };

  var graphToSVGRatio = 0.8;  // Leave some margin.
  var UNCOLORED = 'hsl(0,0%,42%)';

  GraphView.prototype.addGroup = function(className, clipper) {
    var group;
    if (clipper !== undefined) {
      this.root.prepend(clipper.dom);
      group = svg.create('g', { 'class': className, 'clip-path': clipper.url });
    } else {
      group = svg.create('g', { 'class': className });
    }
    this.root.append(group);
    return group;
  };

  GraphView.prototype.addSideSeparators = function(numVisibleSides) {
    if (numVisibleSides <= 1) {
      return;
    }
    var separatorGroup = svg.create('g', {'class': 'side-separators'});
    this.root.append(separatorGroup);
    var sideWidth = this.svg.width() / numVisibleSides;
    for (var i = 0; i < numVisibleSides - 1; ++i) {
      var separatorLine = svg.create('line', {
        x1: sideWidth * (i + 1),
        y1: 0,
        x2: sideWidth * (i + 1),
        y2: this.svg.height(),
        'class': 'side-separator-line',
      });
      separatorGroup.append(separatorLine);
    }
  };

  GraphView.prototype.createClippers = function(halfColumnWidth, numVisibleSides) {
    var clippers = [];
    for (var i = 0; i < numVisibleSides; ++i) {
      var isLeftMost = i === 0;
      var isRightMost = i === (numVisibleSides - 1);
      var separatorWidthHalf = 1;
      var leftOffset = isLeftMost ? 0 : separatorWidthHalf;
      var rightOffset = isRightMost ? 0 : separatorWidthHalf;
      var clipper = new Clipper({
        x: (i * 2) * halfColumnWidth + leftOffset,
        y: 0,
        width: halfColumnWidth * 2 - leftOffset - rightOffset,
        height: 30000,
      });
      this.root.prepend(clipper.dom);
      clippers.push(clipper);
    }
    return clippers;
  };

  GraphView.prototype.update = function(data, menu) {
    this.clear();
    var i;
    var zoom = this.svg.height() * graphToSVGRatio;
    var sides = [this.scope.graph.left, this.scope.graph.right];
    var visibleSides = sides.filter(function(s) { return s && s.graphMode; });
    var halfColumnWidth = this.svg.width() / visibleSides.length / 2;
    this.edgeGroups = [];
    this.vertexGroups = [];
    this.addSideSeparators(visibleSides.length);
    var clippers = this.createClippers(halfColumnWidth, visibleSides.length);
    // The order of adding the groups is important. Whatever comes later, will overlay
    // things defined earlier. We want edges to be overlaid by vertices.
    this.crossEdgeGroup = this.addGroup('edges');
    for (i = 0; i < visibleSides.length; ++i) {
      this.edgeGroups.push(this.addGroup('edges', clippers[i]));
    }
    for (i = 0; i < visibleSides.length; ++i) {
      this.vertexGroups.push(this.addGroup('nodes side' + i, clippers[i]));
    }
    this.legend = svg.create('g', {'class': 'legend'});
    this.root.append(this.legend);
    var oldVertices = this.vertices || new Vertices(this);
    this.vertices = [];  // Sparse, indexed by side. Everything else is indexed by visible side.
    var sideIndices = [];  // Maps from visible index to side index.
    var vsi = 0;
    var vs;
    for (i = 0; i < sides.length; ++i) {
      if (sides[i] && sides[i].graphMode) {
        var xMin = (vsi * 2) * halfColumnWidth;
        var xOff = (vsi * 2 + 1) * halfColumnWidth;
        var xMax = (vsi * 2 + 2) * halfColumnWidth;
        var yOff = this.svg.height() / 2;
        var dataVs = data.vertexSets[vsi];
        if (sides[i].display !== '3d') {
          var offsetter;
          if (oldVertices[i] && oldVertices[i].mode === dataVs.mode) {
            offsetter = oldVertices[i].offsetter.inherit();
            if (oldVertices[i].halfColumnWidth !== halfColumnWidth) {
              offsetter.xOff = xOff;
            }
          } else {
            offsetter = new Offsetter(xOff, yOff, zoom, zoom, menu, xMin, xMax, i);
          }
          if (dataVs.mode === 'sampled') {
            vs = this.addSampledVertices(dataVs, offsetter, sides[i], this.vertexGroups[vsi], i);
          } else {
            vs = this.addBucketedVertices(dataVs, offsetter, sides[i], this.vertexGroups[vsi]);
          }
          vs.offsetter = offsetter;
          vs.xMin = xMin;
          vs.halfColumnWidth = halfColumnWidth;
          vs.clipper = clippers[vsi];
          this.vertices[i] = vs;
          this.sideMouseBindings(offsetter, xMin, xMax);
        }
        vsi += 1;
        sideIndices.push(i);
      }
    }
    var side;
    for (i = 0; i < data.edgeBundles.length; ++i) {
      var e = data.edgeBundles[i];
      // Avoid an error with the Grunt test data, which has edges going to the other side
      // even if we only have one side.
      if (e.srcIdx >= visibleSides.length || e.dstIdx >= visibleSides.length) { continue; }
      side = undefined;
      var edgeGroup = this.crossEdgeGroup;
      if (e.srcIdx === e.dstIdx) {
        var idx = e.srcIdx;
        side = visibleSides[idx];
        edgeGroup = this.edgeGroups[idx];
        if (side.display === '3d') {
          var scope = this.scope.$new();
          scope.edges = e.edges;
          scope.layout3D = e.layout3D;
          scope.width = 2 * halfColumnWidth;
          scope.left = idx * 2 * halfColumnWidth;
          var r = $compile('<renderer></renderer>')(scope);
          this.svg.after(r);
          this.renderers.push(r);
          continue;
        }
        if (this.vertices[sideIndices[idx]].mode === 'sampled' && e.edges.length >= 5) {
          this.vertices[sideIndices[idx]].addLegendLine(e.edges.length + ' edges');
        }
      }
      var src = this.vertices[sideIndices[e.srcIdx]];
      var dst = this.vertices[sideIndices[e.dstIdx]];
      var edges = this.addEdges(e.edges, src, dst, side, edgeGroup);
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
    for (var i = 0; i < from.vs.length; ++i) {
      fromById[from.vs[i].id] = from.vs[i];
    }
    for (i = 0; i < to.vs.length; ++i) {
      var v = to.vs[i];
      var fv = fromById[v.id];
      if (fv) {
        v.x = fv.x;
        v.y = fv.y;
        // Copy frozen status, plus add one more freeze.
        v.frozen = fv.frozen + 1;
      }
    }
  }

  function unfreezeAll(vertices) {
    for (var i = 0; i < vertices.vs.length; ++i) {
      if (vertices.vs[i].frozen) {
        vertices.vs[i].frozen -= 1;
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
    for (i = 0; i < keys.length; ++i) {
      var tiny = tinycolor(keys[i]);
      if (tiny.isValid()) {
        colorMap[keys[i]] = tiny.toString();
      }
    }
    return colorMap;
  }

  // Vertices represents a set of vertices. The Vertex objects are in the "vs" array.
  // A wide range of metadata is also contained in this object, plus some methods.
  function Vertices(graphView) {
    this.gv = graphView;
    this.vs = [];
    this.iconMapping = {};
  }
  // Prepare a label-icon map for these labels, generate legend.
  Vertices.prototype.initIcons = function(attr, labels) {
    var neutrals = ['square', 'hexagon', 'pentagon', 'star', 'triangle', 'circle'];
    function dropNeutral(label) {
      var j = neutrals.indexOf(label);
      if (j !== -1) {
        neutrals.splice(j, 1);
      }
    }
    var mapping = {};
    var unassigned = [];
    var i, label;
    this.addLegendLine('Icon: ' + attr);
    // Assign literals first.
    for (i = 0; i < labels.length; ++i) {
      label = labels[i];
      if (!mapping[label]) {
        if (label === undefined) {
          mapping[label] = 'circle';
          dropNeutral('circle');
          this.addLegendLine('circle: undefined', 10);
        } else if (this.gv.hasIcon(label) && label !== 'circle') {
          mapping[label] = label;
          dropNeutral(label);
        } else if (unassigned.indexOf(label) === -1) {
          unassigned.push(label);
        }
      }
    }
    // Assign neutrals.
    if (neutrals.length === 0) { neutrals = ['circle']; }
    if (unassigned.length <= neutrals.length) {
      for (i = 0; i < unassigned.length; ++i) {
        mapping[unassigned[i]] = neutrals[i];
        this.addLegendLine(neutrals[i] + ': ' + unassigned[i], 10);
      }
    } else {
      var wildcard = neutrals.pop();
      for (i = 0; i < neutrals.length; ++i) {
        mapping[unassigned[i]] = neutrals[i];
        this.addLegendLine(neutrals[i] + ': ' + unassigned[i], 10);
      }
      this.addLegendLine(wildcard + ': other', 10);
    }
    this.iconMapping = mapping;
  };

  Vertices.prototype.getIcon = function(label) {
    return this.gv.getIcon(this.iconMapping[label] || 'circle');
  };

  Vertices.prototype.addLegendLine = function(text, indent) {
    indent = indent || 0;
    var margin = 50;
    var xMargin = margin + indent;
    var x = this.leftOrRight === 'left' ? xMargin : this.gv.svg.width() - xMargin;
    var anchor = this.leftOrRight === 'left' ? 'start' : 'end';
    var i = this.legendNextLine || 0;
    this.legendNextLine = i + 1;
    var legendElement =
      svg.create('text', { 'class': 'legend', x: x, y: i * 22 + margin }).text(text);
    legendElement.attr('text-anchor', anchor);
    this.gv.legend.append(legendElement);
    return legendElement;
  };

  Vertices.prototype.addColorLegend = function(colorMap, title) {
    this.addLegendLine(title);
    for (var attr in colorMap) {
      var l = this.addLegendLine(attr || 'undefined', 10);
      l.attr('style', 'fill: ' + colorMap[attr] || UNCOLORED);
    }
  };

  Vertices.prototype.setupColorMap = function(
      siblings, colorMeta, legendTitle, colorKey) {
    var resultMap;
    if (colorMeta) {
      colorKey = (colorKey === undefined) ? colorMeta.id : colorKey;
      var fullLegendTitle = legendTitle + ': ' + colorMeta.title;
      if (colorMeta.typeName === 'Double') {
        var values = mapByAttr(siblings, colorKey, 'double');
        resultMap = doubleColorMap(values);
        var bounds = common.minmax(values);
        var legendMap = {};
        legendMap['min: ' + bounds.min] = resultMap[bounds.min];
        legendMap['max: ' + bounds.max] = resultMap[bounds.max];
        // only shows the min max values
        this.addColorLegend(legendMap, fullLegendTitle);
      } else if (colorMeta.typeName === 'String') {
        resultMap = stringColorMap(mapByAttr(siblings, colorKey, 'string'));
        this.addColorLegend(resultMap, fullLegendTitle);
      } else {
        /* eslint-disable no-console */
        console.error('The type of ' +
                      colorMeta + ' (' + colorMeta.typeName +
                      ') is not supported for color visualization!');
      }
    }
    return resultMap;
  };

  GraphView.prototype.addSampledVertices = function(data, offsetter, side, vertexGroup, sideIndex) {
    var vertices = new Vertices(this);
    vertices.side = side;
    vertices.mode = 'sampled';
    vertices.offsetter = offsetter;
    vertices.vertexSetId = side.vertexSet.id;
    vertices.leftOrRight = sideIndex === 0 ? 'left' : 'right';

    for (var attr in side.vertexAttrs) {
      if (side.vertexAttrs[attr] !== undefined) {
        // Capitalize.
        var attrLabel = attr.charAt(0).toUpperCase() + attr.slice(1);
        // UnCammelify.
        attrLabel = attrLabel.replace(/([A-Z])/g, ' $1');
        // We handle icon and color attributes separately.
        if (attrLabel.indexOf('Color') === -1 && attrLabel !== ' Icon') {
          vertices.addLegendLine(attrLabel + ': ' + side.vertexAttrs[attr].title);
        }
      }
    }

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

    var colorAttr = (side.vertexAttrs.color) ? side.vertexAttrs.color.id : undefined;
    var colorMap = vertices.setupColorMap(data.vertices, side.vertexAttrs.color, 'Vertex Color');

    var labelColorAttr = (side.vertexAttrs.labelColor) ? side.vertexAttrs.labelColor.id : undefined;
    var labelColorMap = vertices.setupColorMap(
      data.vertices, side.vertexAttrs.labelColor, 'Label Color');

    var opacityAttr = (side.vertexAttrs.opacity) ? side.vertexAttrs.opacity.id : undefined;
    var opacityMax = 1;
    if (opacityAttr) {
      var opacityBounds = common.minmax(mapByAttr(data.vertices, opacityAttr, 'double'));
      opacityMax = opacityBounds.max;
    }

    var i, vertex, label, labelColor, icon, image;
    if (side.vertexAttrs.icon) {
      // Collect icon strings for icon assignment.
      var iconStrings = [];
      for (i = 0; i < data.vertices.length; ++i) {
        vertex = data.vertices[i];
        icon = vertex.attrs[side.vertexAttrs.icon.id];
        if (icon.defined) {
          iconStrings.push(icon.string);
        } else {
          iconStrings.push(undefined);
        }
      }
      vertices.initIcons(side.vertexAttrs.icon.title, iconStrings);
    }

    for (i = 0; i < data.vertices.length; ++i) {
      vertex = data.vertices[i];

      label = undefined;
      if (side.vertexAttrs.label) {
        label = vertex.attrs[side.vertexAttrs.label.id].string;
      }

      var size = 0.5;
      if (sizeAttr) {
        var s = vertex.attrs[sizeAttr].double;
        size = s > 0 ? s / sizeMax : 0;
      }

      var labelSize = 0.5;
      if (labelSizeAttr) {
        var l = vertex.attrs[labelSizeAttr].double;
        labelSize = l > 0 ? l / labelSizeMax : 0;
      }

      var color = UNCOLORED;
      if (colorAttr && vertex.attrs[colorAttr].defined) {
        // in case of doubles the keys are strings converted from the DynamicValue's double field
        // we can't just use the string field of the DynamicValue as 1.0 would turn to '1'
        color = (side.vertexAttrs.color.typeName === 'Double') ?
          colorMap[vertex.attrs[colorAttr].double] : colorMap[vertex.attrs[colorAttr].string];
      }

      labelColor = undefined;
      if (labelColorAttr && vertex.attrs[labelColorAttr].defined) {
        // in case of doubles the keys are strings converted from the DynamicValue's double field
        // we can't just use the string field of the DynamicValue as 1.0 would turn to '1'
        labelColor = (side.vertexAttrs.labelColor.typeName === 'Double') ?
          labelColorMap[vertex.attrs[labelColorAttr].double] :
          labelColorMap[vertex.attrs[labelColorAttr].string];
      }

      var opacity = 1;
      if (opacityAttr) { opacity = vertex.attrs[opacityAttr].double / opacityMax; }

      icon = undefined;
      if (side.vertexAttrs.icon) { icon = vertex.attrs[side.vertexAttrs.icon.id].string; }
      image = undefined;
      if (side.vertexAttrs.image) { image = vertex.attrs[side.vertexAttrs.image.id].string; }

      var radius = 0.1 * Math.sqrt(size);
      var v = new Vertex(vertices,
                         vertex,
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
      vertices.vs.push(v);

      this.sampledVertexMouseBindings(vertices, v, vertexGroup);
      vertexGroup.append(v.dom);
    }

    if (data.vertices.length >= 5) {
      vertices.addLegendLine(data.vertices.length + ' vertices');
    }
    return vertices;
  };

  function translateTouchToMouseEvent(ev) {
    if (ev.type === 'touchmove') {
      // Translate into mouse event.
      ev.pageX = ev.originalEvent.changedTouches[0].pageX;
      ev.pageY = ev.originalEvent.changedTouches[0].pageY;
      ev.preventDefault();
    }
  }

  GraphView.prototype.sampledVertexMouseBindings = function(vertices, vertex, vertexGroup) {
    var scope = this.scope;
    var svgElement = this.svg;
    var offsetter = vertex.offsetter;
    function modelX(pageX) {
      return (pageX - svgElement.offset().left - offsetter.xOff) / offsetter.zoom;
    }
    function modelY(pageY) {
      return (pageY - svgElement.offset().top - offsetter.yOff) / offsetter.zoom;
    }
    vertex.dom.on('mousedown touchstart', function(evStart) {
      evStart.stopPropagation();
      translateTouchToMouseEvent(evStart);
      vertex.hold();
      vertex.xDragStart = modelX(evStart.pageX);
      vertex.yDragStart = modelY(evStart.pageY);
      vertex.xBeforeDrag = vertex.x;
      vertex.yBeforeDrag = vertex.y;
      vertex.dragged = false;
      vertexGroup.append(vertex.dom);  // Bring to top.
      angular.element(window).on('mouseup touchend', function() {
        angular.element(window).off('mousemove mouseup touchmove touchend');
        if (!vertex.held) {
          return;  // Duplicate event.
        }
        vertex.release();
        if (vertex.dragged) {  // It was a drag.
          vertices.animate();
        } else {  // It was a click.
          scope.$apply(function() {
            var actions = [];
            var side = vertices.side;
            var id = vertex.id.toString();
            var attributes = {};
            // Iterate through the visualization modes (e.g. label, size) to get the list of
            // attributes which are visualized.
            for (var visMode in side.vertexAttrs) {
              var attribute = side.vertexAttrs[visMode];
              if (attribute !== undefined) {
                attributes[attribute.title] = vertex.data.attrs[attribute.id].string;
              }
            }

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
              attributes: attributes,
            });
          });
        }
      });
      angular.element(window).on('mousemove touchmove', function(ev) {
        if (vertex.positioned) { return; }
        translateTouchToMouseEvent(ev);
        var ex = modelX(ev.pageX);
        var ey = modelY(ev.pageY);
        vertex.dragged = ex !== vertex.xDragStart || ey !== vertex.yDragStart;
        var x = vertex.xBeforeDrag + ex - vertex.xDragStart;
        var y = vertex.yBeforeDrag + ey - vertex.yDragStart;
        vertex.moveTo(x, y);
        vertex.forceOX = x;
        vertex.forceOY = y;
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
      var oe = e.originalEvent;
      var mx = oe.pageX - svgElement.offset().left;
      var my = oe.pageY - svgElement.offset().top;
      if ((mx < xMin) || (mx >= xMax)) {
        return;
      }
      e.preventDefault();
      var deltaX = oe.deltaX;
      var deltaY = oe.deltaY;
      if (/Firefox/.test(window.navigator.userAgent)) {
        // Every browser sets different deltas for the same amount of scrolling.
        // It is tiny on Firefox. We need to boost it.
        deltaX *= 20;
        deltaY *= 20;
      }
      var plainScroll = oe.shiftKey ? 0 : deltaY;
      var shiftScroll = deltaX + (oe.shiftKey ? deltaY : 0);
      zoom({ x: mx, y: my }, plainScroll, shiftScroll);
    });
    this.svgDoubleClickListeners.push(function(e) {
      var oe = e.originalEvent;
      var mx = oe.pageX - svgElement.offset().left;
      var my = oe.pageY - svgElement.offset().top;
      if ((mx < xMin) || (mx >= xMax)) {
        return;
      }
      e.preventDefault();
      // Left/right is in/out.
      var scroll = e.which === 1 ? -500 : 500;
      // Shift affects thickness.
      var shift = oe.shiftKey;
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
    var xb = common.minmax(vertices.vs.map(function(v) { return v.x; }));
    var yb = common.minmax(vertices.vs.map(function(v) { return v.y; }));
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
          vertices.vs.map(function(v) { return (v.data.attrs[sliderAttr.id] || {}).double; }));
      var pos = Number(sliderPos());
      for (var i = 0; i < vertices.vs.length; ++i) {
        var v = vertices.vs[i];
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
    this.group = svg.create('g', { 'class': 'map', 'clip-path': vertices.clipper.url });
    this.gv.root.prepend(this.group);
    // The size of the Earth in lat/long view. It doesn't make much difference,
    // just has to be a reasonable value to avoid too small/too large numbers.
    this.GLOBE_SIZE = 500;
    // Constant to match Google Maps projection.
    this.GM_MULT = 0.403;
    // How much to wait after pan/zoom events before requesting a new map.
    this.NAVIGATION_DELAY = 100;  // Milliseconds.
    this.root = 'https://maps.googleapis.com/maps/api/staticmap?';
    this.key = 'AIzaSyBcML5zQetjkRFuqpSSG6EmhS2vSWRssZ4';  // big-graph-gc1 API key.
    this.images = [];
    this.vertices.offsetter.rule(this);
    var that = this;
    var unwatch = util.deepWatch(this.gv.scope, 'mapFilters', function() { that.update(); });
    this.gv.unregistration.push(function() {
      unwatch();
    });
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
    var filters = this.gv.scope.mapFilters;
    var style = 'feature:all' +
      // Map gamma to the [0.1, 10] range using an exponential scale.
      '|gamma:' + Math.pow(10, filters.gamma / 100) +
      '|saturation:' + filters.saturation +
      '|lightness:' + filters.brightness;
    var href = (
      this.root + 'center=' + clat + ',' + clon + '&zoom=' + zoomLevel +
      '&key=' + this.key +
      '&language=en' +
      '&size=640x640&scale=2&style=' + style);
    image[0].setAttributeNS('http://www.w3.org/1999/xlink', 'href', href);
    image.size = this.GLOBE_SIZE * Math.pow(2, -zoomLevel) / this.GM_MULT;
    image.x = x - image.size / 2;
    image.y = y - image.size / 2;
    // Disable drag & drop on Firefox.
    image.on('dragstart', function() { return false; });
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
    for (var i = 0; i < vertices.vs.length; ++i) {
      var v = vertices.vs[i];
      v.degree = 0;
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
        e.src.degree += 1;
        e.dst.degree += 1;
      }
    }
    var scale = this.svg.height();
    var engine = new FORCE_LAYOUT.Engine({
      attraction: 0.01,
      repulsion: scale,
      gravity: 0.05,
      drag: 0.2,
      labelAttraction: parseFloat(vertices.side.animate.labelAttraction),
      style: vertices.side.animate.style,
    });
    // Generate initial layout for 2 seconds or until it stabilizes.
    var t1 = Date.now();
    /* eslint-disable no-empty */
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
      var animate = vertices.side.animate;
      // This also ends up getting called when the side is closed due to the deep watch.
      // Accept this silently.
      if (!animate) { return; }
      engine.opts.labelAttraction = parseFloat(animate.labelAttraction);
      engine.opts.style = animate.style;
      if (animating && animate.enabled && engine.step(vertices)) {
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

  GraphView.prototype.addBucketedVertices = function(data, offsetter, viewData, vertexGroup) {
    var vertices = new Vertices(this);
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
      vertexGroup.append(l.dom);
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
      vertexGroup.append(l.dom);
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
      vertexGroup.append(l.dom);
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
      vertexGroup.append(l.dom);
    }

    var sizes = data.vertices.map(function(n) { return n.size; });
    var vertexScale = 1 / common.minmax(sizes).max;
    for (i = 0; i < data.vertices.length; ++i) {
      var vertex = data.vertices[i];
      var radius = 0.1 * Math.sqrt(vertexScale * vertex.size);
      var v = new Vertex(vertices,
                         vertex,
                         common.normalize(vertex.x + 0.5, xNumBuckets),
                         -common.normalize(vertex.y + 0.5, yNumBuckets),
                         radius,
                         vertex.size);
      offsetter.rule(v);
      vertices.vs.push(v);
      if (vertex.size === 0) {
        continue;
      }
      this.bucketedVertexMouseBindings(vertices, v);
      vertexGroup.append(v.dom);
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

  GraphView.prototype.addEdges = function(edges, srcs, dsts, side, edgeGroup) {
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
      widthKey = attrKey(side.edgeAttrs.width);
      colorKey = attrKey(side.edgeAttrs.edgeColor);
      colorMap = srcs.setupColorMap(
        edges, side.edgeAttrs.edgeColor, 'Edge Color', colorKey);
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
    var color, label;
    for (var i = 0; i < edges.length; ++i) {
      var edge = edges[i];
      var width = edgeWidths[i];
      if (edge.size === 0) {
        continue;
      }
      var a = srcs.vs[edge.a];
      var b = dsts.vs[edge.b];

      color = undefined;
      if (colorKey && edge.attrs[colorKey].defined) {
        color = (side.edgeAttrs.edgeColor.typeName === 'Double') ?
          colorMap[edge.attrs[colorKey].double] : colorMap[edge.attrs[colorKey].string];
      }
      label = undefined;
      if (labelKey) {
        label = edge.attrs[labelKey].string;
      }
      var e = new Edge(a, b, edgeScale * width, color, label);
      edgeObjects.push(e);
      edgeGroup.append(e.dom);
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

  function Vertex(
      vertices, data, x, y, r, text, textSize, color, opacity, labelColor, icon, image) {
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
      this.icon = vertices.getIcon(icon);
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
    this.icon[0].setAttribute('transform',
      svgTranslate(sx, sy) +
      svgScale(r * this.icon.scale) +
      svgTranslate(-this.icon.center.x, -this.icon.center.y));
    this.label[0].setAttribute('x', sx);
    this.label[0].setAttribute('y', sy);
    this.touch[0].setAttribute('cx', sx);
    this.touch[0].setAttribute('cy', sy);
    this.touch[0].setAttribute('r', r);

    var backgroundWidth = this.labelBackground[0].getAttribute('width');
    var backgroundHeight = this.labelBackground[0].getAttribute('height');
    this.labelBackground[0].setAttribute('x', sx - backgroundWidth / 2);
    this.labelBackground[0].setAttribute('y', sy - backgroundHeight / 2);
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
  Edge.prototype.setVisible = function(visible) {
    if (visible) {
      svg.removeClass(this.dom, 'invisible-edge');
    } else {
      svg.addClass(this.dom, 'invisible-edge');
    }
  };
  Edge.prototype.toFront = function() {
    this.dom.parent().append(this.dom);
  };
  Edge.prototype.reposition = function() {
    function isInside(vertex) {
      return vertex.screenX() >= vertex.offsetter.xMin &&
          vertex.screenX() <= vertex.offsetter.xMax;
    }
    var src = this.src, dst = this.dst;
    this.setVisible(
        src.offsetter.side === dst.offsetter.side || (isInside(src) && isInside(dst)));
    var avgZoom = 0.5 * (src.offsetter.thickness + dst.offsetter.thickness);
    var arrows = svg.arrows(src.screenX(), src.screenY(), dst.screenX(), dst.screenY(), avgZoom);
    this.first[0].setAttribute('d', arrows[0]);
    this.first[0].setAttribute('stroke-width', avgZoom * this.w);
    this.second[0].setAttribute('d', arrows[1]);
    this.second[0].setAttribute('stroke-width', avgZoom * this.w);
    var arcParams = svg.arcParams(
      src.screenX(), src.screenY(), dst.screenX(), dst.screenY(), avgZoom);
    this.label[0].setAttribute('x', arcParams.x);
    this.label[0].setAttribute('y', arcParams.y);
  };

  return directive;
});
