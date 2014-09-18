'use strict';

angular.module('biggraph').directive('graphView', function($window) {
  /* global SVG_UTIL, COMMON_UTIL, FORCE_LAYOUT */
  var svg = SVG_UTIL;
  var util = COMMON_UTIL;
  var directive = {
      template: '<svg class="graph-view" version="1.1" xmlns="http://www.w3.org/2000/svg"></svg>',
      scope: { graph: '=', left: '=', right: '=' },
      replace: true,
      link: function(scope, element) {
        var gv = new GraphView(scope, element);
        function updateGraph() {
          if (scope.graph === undefined || !scope.graph.$resolved) {
            gv.loading();
          } else if (scope.graph.error) {
            gv.error(scope.graph.error);
          } else {
            gv.update(scope.graph);
          }
        }
        scope.$watch('graph', updateGraph, true);
        angular.element($window).bind('resize', updateGraph);
      },
    };

  function Offsetter(xOff, yOff) {
    this.xOff = xOff;
    this.yOff = yOff;
    this.elements = [];
  }
  Offsetter.prototype.rule = function(element) {
    this.elements.push(element);
    var that = this;
    element.screenX = function() {
      return element.x + that.xOff;
    };
    element.screenY = function() {
      return element.y + that.yOff;
    };
    element.reDraw();
  };
  Offsetter.prototype.panTo = function(x, y) {
    this.xOff = x;
    this.yOff = y;
    for (var i = 0; i < this.elements.length; ++i) {
      this.elements[i].reDraw();
    }
  };

  function GraphView(scope, element) {
    this.scope = scope;
    this.unwatch = [];  // Watchers to be removed when drawing new graph.
    this.svg = angular.element(element);
    this.svg.append([svg.marker('arrow'), svg.marker('arrow-highlight-in'), svg.marker('arrow-highlight-out')]);
    this.root = svg.create('g', {'class': 'root'});
    var graphToSVGRatio = 0.67;
    this.zoom = 500 * graphToSVGRatio; // todo: replace 500 with the actual svg height
    this.svg.append(this.root);
  }

  GraphView.prototype.loading = function() {
    this.root.empty();
    var x = this.svg.width() / 2, y = 250;
    var w = 5000, h = 500;
    var loading = svg.create('rect', {'class': 'loading', width: w, height: h, x: x - w/2, y: y - h/2});
    var anchor = ' ' + x + ' ' + y;
    var rotate = svg.create('animateTransform', {
      attributeName: 'transform',
      type: 'rotate',
      from: '0' + anchor,
      to: '360' + anchor,
      dur: '3s',
      repeatCount: 'indefinite',
    });
    loading.append(rotate);
    this.root.append(loading);
  };

  GraphView.prototype.error = function(msg) {
    this.root.empty();
    var x = this.svg.width() / 2, y = 250;
    var text = svg.create('text', {'class': 'error', x: x, y: y, 'text-anchor': 'middle'});
    var maxLength = 100;  // The error message can be very long and SVG does not wrap text.
    for (var i = 0; i < msg.length; i += maxLength) {
      text.append(svg.create('tspan', {x: x, dy: 30}).text(msg.substring(i, i + maxLength)));
    }
    this.root.append(text);
  };

  GraphView.prototype.update = function(data) {
    // Remove old watchers.
    for (var i = 0; i < this.unwatch.length; ++i) {
      this.unwatch[i]();
    }
    var sides = [this.scope.left, this.scope.right];
    this.root.empty();
    this.edges = svg.create('g', {'class': 'edges'});
    this.vertices = svg.create('g', {'class': 'nodes'});
    this.root.append([this.edges, this.vertices]);
    var vertices = [];
    var vsIndex = 0;
    var halfColumnWidth = this.svg.width() / sides.length / 2;
    for (i = 0; i < sides.length; ++i) {
      if (sides[i] && sides[i].graphMode) {
        var xMin = (i * 2) * halfColumnWidth;
        var xOff = (i * 2 + 1) * halfColumnWidth;
        var xMax = (i * 2 + 2) * halfColumnWidth;
        var yOff = 500 / 2; // todo: replace 500 with the actual svg height
        var vs = data.vertexSets[vsIndex];
        vsIndex += 1;
        var offsetter = new Offsetter(xOff, yOff);
        if (vs.mode === 'sampled') {
          vertices.push(this.addSampledVertices(vs, offsetter, sides[i]));
        } else {
          vertices.push(this.addBucketedVertices(vs, offsetter));
        }
        this.sideMouseBindings(offsetter, xMin, xMax);
      }
    }
    for (i = 0; i < data.edgeBundles.length; ++i) {
      var e = data.edgeBundles[i];
      var edges = this.addEdges(e.edges, vertices[e.srcIdx], vertices[e.dstIdx]);
      if (e.srcIdx === e.dstIdx) {
        vertices[e.srcIdx].edges = edges;
      }
    }
    for (i = 0; i < vertices.length; ++i) {
      if (vertices[i].mode === 'sampled') {
        this.layout(vertices[i]);
      }
    }
  };

  function mapByAttr(vert, attr) {
    return vert.map(function(n) {
      return n.attrs[attr];
    });
  }

  GraphView.prototype.addSampledVertices = function(data, offsetter, side) {
    var vertices = [];
    vertices.side = side;
    vertices.mode = 'sampled';

    var size = (side.attrs.size) ? side.attrs.size.id : undefined;
    var vertexSizeScale;
    if (size) {
      var vertexSizeBounds = util.minmax(mapByAttr(data.vertices, size));
      vertexSizeScale = this.zoom * 2 / vertexSizeBounds.max;
    }

    var color = (side.attrs.color) ? side.attrs.color.id : undefined;
    var vertexColorBounds, vertexColorScale, colorMap;
    if (color) {
      if (side.attrs.color.typeName === 'Double') {
        vertexColorBounds = util.minmax(mapByAttr(data.vertices, color));
        vertexColorScale =
          100 / Math.max(vertexColorBounds.max, Math.abs(vertexColorBounds.min));
      } else if (side.attrs.color.typeName === 'String') {
        var enumMap = {};
        colorMap = {};
        angular.forEach(data.vertices, function(n) {
          enumMap[n.attrs[color]] =
            (enumMap[n.attrs[color]]) ? enumMap[n.attrs[color]] + 1 : 1;
        });
        var cdist = Math.floor(360 / Object.keys(enumMap).length);
        var ci = 0;
        angular.forEach(enumMap, function(v, k) { colorMap[k] = ci; ci += cdist; });
        console.log(colorMap);
      }
    }

    for (var i = 0; i < data.vertices.length; ++i) {
      var vertex = data.vertices[i];

      var label = (side.attrs.label) ? vertex.attrs[side.attrs.label.id] || vertex.id : vertex.id;

      // todo: set a minimum size for 0 and undefined vertices here
      var vertexSize =
        (size) ? Math.sqrt(vertexSizeScale * vertex.attrs[size]) || 0 : Math.sqrt(this.zoom * 2);

      var hslColor, h, s, l;
      if (color && side.attrs.color.typeName === 'Double') {
        // negative is blue, positive is red, zero lighter grey
        h = (vertex.attrs[color] >= 0) ? 0 : 240;
        s = Math.abs(vertexColorScale * vertex.attrs[color]);
        l = (vertexColorScale) ? 50 : 25; // default color is dark grey
      } else if (color && side.attrs.color.typeName === 'String') {
        h = colorMap[vertex.attrs[color]];
        s = 100;
        l = 42;
      } else {
        h = 0;
        s = 0;
        l = 25;
      }
      hslColor = 'hsl(' + h + ',' + s + '%,' + l + '%)';

      var v = new Vertex(Math.random() * 400 - 200,
                         Math.random() * 400 - 200,
                         vertexSize,
                         label,
                         hslColor);
      offsetter.rule(v);
      v.id = vertex.id;
      svg.addClass(v.dom, 'sampled');
      if (side.center.indexOf(v.id) > -1) {
        svg.addClass(v.dom, 'center');
      }
      vertices.push(v);
      if (vertexSize === 0) {
        continue;
      }
      this.sampledVertexMouseBindings(vertices, v, offsetter);
      this.vertices.append(v.dom);
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

  GraphView.prototype.sampledVertexMouseBindings = function(vertices, vertex, offsetter) {
    var scope = this.scope;
    var svgElement = this.svg;
    function setCenter() {
      scope.$apply(function() {
        vertices.side.setCenter(vertex.id);
      });
    }
    vertex.dom.on('mousedown touchstart', function(evStart) {
      evStart.stopPropagation();
      vertex.held = true;
      vertex.dragged = false;
      angular.element(window).on('mouseup touchend', function() {
        angular.element(window).off('mousemove mouseup touchmove touchend');
        if (!vertex.held) {
          return;  // Duplicate event.
        }
        vertex.held = false;
        if (vertex.dragged) {  // It was a drag.
          vertex.dragged = false;
          vertices.animate();
        } else {  // It was a click.
          setCenter();
        }
      });
      angular.element(window).on('mousemove touchmove', function(ev) {
        translateTouchToMouseEvent(ev);
        var x = ev.pageX - svgElement.offset().left - offsetter.xOff;
        var y = ev.pageY - svgElement.offset().top - offsetter.yOff;
        vertex.moveTo(x, y);
        vertex.forceOX = x;
        vertex.forceOY = y;
        vertex.dragged = true;
        vertices.animate();
      });
    });
  };

  GraphView.prototype.sideMouseBindings = function(offsetter, xMin, xMax) {
    var svgElement = this.svg;
    svgElement.on('mousedown touchstart', function(evStart) {
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
  };

  GraphView.prototype.layout = function(vertices) {
    for (var i = 0; i < vertices.length; ++i) {
      var v = vertices[i];
      v.forceMass = 1;
      v.forceOX = v.x;
      v.forceOY = v.y;
    }
    for (i = 0; i < vertices.edges.length; ++i) {
      var e = vertices.edges[i];
      e.src.forceMass += 1;
      e.dst.forceMass += 1;
    }
    var engine = new FORCE_LAYOUT.Engine({ attraction: 0.01, repulsion: 500, gravity: 0.05, drag: 0.2 });
    // Initial layout.
    var t1 = Date.now();
    while (engine.step(vertices) && Date.now() - t1 <= 2000) {}
    var animating = false;
    // Call vertices.animate() later to trigger interactive layout.
    vertices.animate = function() {
      if (!animating) {
        animating = true;
        window.requestAnimationFrame(vertices.step);
      }
    };
    vertices.step = function() {
      if (vertices.side.animate && engine.step(vertices)) {
        window.requestAnimationFrame(vertices.step);
      } else {
        animating = false;
      }
    };
    vertices.animate();
    // Kick off animation when the user manually turns it on.
    // (This watcher is unregistered when a new graph is loaded.)
    this.unwatch.push(this.scope.$watch(
        function() { return vertices.side.animate; },
        function() { vertices.animate(); }));
  };

  GraphView.prototype.addBucketedVertices = function(data, offsetter) {
    var vertices = [];
    var xLabels = [], yLabels = [];
    var i, x, y, l, side;
    var labelSpace = 50;
    y = this.zoom * 0.5 + labelSpace;
    
    var xb = util.minmax(data.vertices.map(function(n) { return n.x; }));
    var yb = util.minmax(data.vertices.map(function(n) { return n.y; }));
    
    var xNumBuckets = xb.span + 1;
    var yNumBuckets = yb.span + 1;
    
    for (i = 0; i < data.xLabels.length; ++i) {
      if (data.xLabelType === 'between') {
        x = this.zoom * util.normalize(i, xNumBuckets);
      } else {
        x = this.zoom * util.normalize(i + 0.5, xNumBuckets);
      }
      l = new Label(x, y, data.xLabels[i], vertices);
      offsetter.rule(l);
      xLabels.push(l);
      this.vertices.append(l.dom);
    }
    // Labels on the left on the left and on the right on the right.
    if (offsetter.xOff < this.svg.width() / 2) {
      x = 0 - this.zoom * 0.5 - labelSpace;
      side = 'left';
    } else {
      x = this.zoom * 0.5 + labelSpace;
      side = 'right';
    }
    for (i = 0; i < data.yLabels.length; ++i) {
      if (data.yLabelType === 'between') {
        y = this.zoom * util.normalize(i, yNumBuckets);
      } else {
        y = this.zoom * util.normalize(i + 0.5, yNumBuckets);
      }
      l = new Label(x, y, data.yLabels[i], side);
      offsetter.rule(l);
      yLabels.push(l);
      this.vertices.append(l.dom);
    }
     
    var vertexScale = this.zoom * 2 / util.minmax(data.vertices.map(function(n) { return n.size; })).max;
    for (i = 0; i < data.vertices.length; ++i) {
      var vertex = data.vertices[i];
      var v = new Vertex(this.zoom * util.normalize(vertex.x + 0.5, xNumBuckets),
                         this.zoom * util.normalize(vertex.y + 0.5, yNumBuckets),
                         Math.sqrt(vertexScale * vertex.size),
                         vertex.size,
                         vertices);
      offsetter.rule(v);
      vertices.push(v);
      if (vertex.size === 0) {
        continue;
      }
      this.vertices.append(v.dom);
      if (xLabels.length !== 0) {
        v.addHoverListener(xLabels[vertex.x]);
        if (data.xLabelType === 'between') { v.addHoverListener(xLabels[vertex.x + 1]); }
      }
      if (yLabels.length !== 0) {
        v.addHoverListener(yLabels[vertex.y]);
        if (data.yLabelType === 'between') { v.addHoverListener(yLabels[vertex.y + 1]); }
      }
    }
    return vertices;
  };

  GraphView.prototype.addEdges = function(edges, srcs, dsts) {
    var edgeObjects = [];
    var bounds = util.minmax(edges.map(function(n) { return n.size; }));
    var normalWidth = this.zoom * 0.02;
    var info = bounds.span / bounds.max;  // Information content of edge widths. (0 to 1)
    // Go up to 3x thicker lines if they are meaningful.
    var edgeScale = normalWidth * (1 + info * 2) / bounds.max;
    for (var i = 0; i < edges.length; ++i) {
      var edge = edges[i];
      if (edgeScale * edge.size < 0.1) {
        continue;
      }
      var a = srcs[edge.a];
      var b = dsts[edge.b];
      var e = new Edge(a, b, edgeScale * edge.size, this.zoom);
      edgeObjects.push(e);
      this.edges.append(e.dom);
    }
    return edgeObjects;
  };

  function Label(x, y, text, side) {
    var labelClass = 'bucket ' + (side || '');
    this.x = x;
    this.y = y;
    this.dom = svg.create('text', {'class': labelClass}).text(text);
  }
  Label.prototype.on = function() { svg.addClass(this.dom, 'highlight'); };
  Label.prototype.off = function() { svg.removeClass(this.dom, 'highlight'); };
  Label.prototype.reDraw = function() {
    this.dom.attr({x: this.screenX(), y: this.screenY()});
  };

  function Vertex(x, y, r, text, color) {
    this.x = x;
    this.y = y;
    this.r = r;
    this.color = color || '#444';
    this.highlight = 'white';
    this.circle = svg.create('circle', {r: r, style: 'fill: ' + this.color});
    var minTouchRadius = 10;
    if (r < minTouchRadius) {
      this.touch = svg.create('circle', {r: minTouchRadius, 'class': 'touch'});
    } else {
      this.touch = this.circle;
    }
    this.label = svg.create('text').text(text);
    this.dom = svg.group([this.circle, this.touch, this.label], {'class': 'vertex' });
    this.moveListeners = [];
    this.hoverListeners = [];
    var that = this;
    this.touch.mouseenter(function() {
      svg.addClass(that.dom, 'highlight');
      that.circle.attr({style: 'fill: ' + that.highlight});
      for (var i = 0; i < that.hoverListeners.length; ++i) {
        that.hoverListeners[i].on(that);
      }
    });
    this.touch.mouseleave(function() {
      svg.removeClass(that.dom, 'highlight');
      that.circle.attr({style: 'fill: ' + that.color});
      for (var i = 0; i < that.hoverListeners.length; ++i) {
        that.hoverListeners[i].off(that);
      }
    });
  }
  // Hover listeners must have an `on()` and an `off()` method.
  Vertex.prototype.addHoverListener = function(hl) {
    this.hoverListeners.push(hl);
  };
  Vertex.prototype.addMoveListener = function(ml) {
    this.moveListeners.push(ml);
  };
  Vertex.prototype.moveTo = function(x, y) {
    this.x = x;
    this.y = y;
    this.reDraw();
  };
  Vertex.prototype.reDraw = function() {
    this.circle.attr({cx: this.screenX(), cy: this.screenY()});
    this.touch.attr({cx: this.screenX(), cy: this.screenY()});
    this.label.attr({x: this.screenX(), y: this.screenY()});
    for (var i = 0; i < this.moveListeners.length; ++i) {
      this.moveListeners[i](this);
    }
  };

  function Edge(src, dst, w, zoom) {
    this.src = src;
    this.dst = dst;
    this.first = svg.create('path', {'class': 'first', 'stroke-width': w});
    this.second = svg.create('path', {'class': 'second', 'stroke-width': w});
    this.dom = svg.group([this.first, this.second], {'class': 'edge'});
    var that = this;
    src.addMoveListener(function() { that.reposition(zoom); });
    dst.addMoveListener(function() { that.reposition(zoom); });
    this.reposition(zoom);
    src.addHoverListener({on: function() { svg.addClass(that.dom, 'highlight-out'); that.toFront(); },
                          off: function() { svg.removeClass(that.dom, 'highlight-out'); }});
    if (src !== dst) {
      dst.addHoverListener({on: function() { svg.addClass(that.dom, 'highlight-in'); that.toFront(); },
                            off: function() { svg.removeClass(that.dom, 'highlight-in'); }});
    }
  }
  Edge.prototype.toFront = function() {
    this.dom.parent().append(this.dom);
  };
  Edge.prototype.reposition = function(zoom) {
    this.first.attr(
      'd',
      svg.arrow1(
        this.src.screenX(), this.src.screenY(), this.dst.screenX(), this.dst.screenY(), zoom));
    this.second.attr(
      'd',
      svg.arrow2(
        this.src.screenX(), this.src.screenY(), this.dst.screenX(), this.dst.screenY(), zoom));
  };

  return directive;
});
