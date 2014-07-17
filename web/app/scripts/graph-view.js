'use strict';

angular.module('biggraph').directive('graphView', function($window) {
  /* global SVG_UTIL, COMMON_UTIL */
  var svg = SVG_UTIL;
  var util = COMMON_UTIL;
  var directive = {
      template: '<svg class="graph-view" version="1.1" xmlns="http://www.w3.org/2000/svg"></svg>',
      scope: { ngModel: '=', state: '=' },
      replace: true,
      link: function(scope, element) {
        var gv = new GraphView(scope, element);
        function updateGraph() {
          if (scope.ngModel === undefined || !scope.ngModel.$resolved) {
            gv.loading();
          } else if (scope.ngModel.error) {
            gv.error(scope.ngModel.error);
          } else {
            gv.update(scope.ngModel);
          }
        }
        scope.$watch('ngModel', updateGraph, true);
        angular.element($window).bind('resize', updateGraph);
      },
    };

  function GraphView(scope, element) {
    this.scope = scope;
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
    var sides = [];
    if (this.scope.state.left.graphMode) { sides.push(this.scope.state.left); }
    if (this.scope.state.right.graphMode) { sides.push(this.scope.state.right); }
    this.root.empty();
    this.edges = svg.create('g', {'class': 'edges'});
    this.vertices = svg.create('g', {'class': 'nodes'});
    this.root.append([this.edges, this.vertices]);
    var vertices = [];
    var n = data.vertexSets.length;
    if (n !== sides.length) {
      console.log('ERROR: ' + n + ' vertex sets for ' + sides.length + ' sides.');
    }
    for (var i = 0; i < n; ++i) {
      var xOff = (i * 2 + 1) * this.svg.width() / n / 2;
      var yOff = 500 / 2; // todo: replace 500 with the actual svg height
      var vs = data.vertexSets[i];
      if (vs.mode === 'sampled') {
        vertices.push(this.addSampledVertices(vs, xOff, yOff, sides[i]));
      } else {
        vertices.push(this.addBucketedVertices(vs, xOff, yOff));
      }
    }
    for (i = 0; i < data.edgeBundles.length; ++i) {
      var e = data.edgeBundles[i];
      this.addEdges(e.edges, vertices[e.srcIdx], vertices[e.dstIdx]);
    }
  };

  GraphView.prototype.addSampledVertices = function(data, xOff, yOff, side) {
    var vertices = [];
    var vertexBounds = util.minmax(data.vertices.map(function(n) { return n.size; }));
    var vertexScale = this.zoom * 2 / vertexBounds.max;
    for (var i = 0; i < data.vertices.length; ++i) {
      var vertex = data.vertices[i];
      // Use vertex.label if set. Use vertex.size if it's not all 1s. Use vertex.id otherwise.
      var label = vertex.id;
      if (vertexBounds.min !== 1 || vertexBounds.max !== 1) { label = vertex.size; }
      label = vertex.label || label;
      var v = new Vertex(xOff + Math.random() * 400 - 200,
                         yOff + Math.random() * 400 - 200,
                         Math.sqrt(vertexScale * vertex.size),
                         label);
      svg.addClass(v.dom, 'sampled');
      vertices.push(v);
      if (vertex.size === 0) {
        continue;
      }
      this.bindSampleClick(v, vertex, side);
      this.vertices.append(v.dom);
    }
    return vertices;
  };

  GraphView.prototype.bindSampleClick = function(v, vertex, side) {
    var scope = this.scope;
    v.dom.click(function() {
      scope.$apply(function() {
        side.center = vertex.id;
      });
    });
  };

  GraphView.prototype.addBucketedVertices = function(data, xOff, yOff) {
    var vertices = [];
    var xLabels = [], yLabels = [];
    var i, x, y, l, side;
    var labelSpace = 50;
    y = yOff + this.zoom * 0.5 + labelSpace;
    
    var xb = util.minmax(data.vertices.map(function(n) { return n.x; }));
    var yb = util.minmax(data.vertices.map(function(n) { return n.y; }));
    
    var xNumBuckets = xb.span + 1;
    var yNumBuckets = yb.span + 1;
    
    for (i = 0; i < data.xLabels.length; ++i) {
      if (data.xLabelType === 'between') {
        x = xOff + this.zoom * util.normalize(i, xNumBuckets);
      } else {
        x = xOff + this.zoom * util.normalize(i + 0.5, xNumBuckets);
      }
      l = new Label(x, y, data.xLabels[i]);
      xLabels.push(l);
      this.vertices.append(l.dom);
    }
    // Labels on the left on the left and on the right on the right.
    if (xOff < this.svg.width() / 2) {
      x = xOff - this.zoom * 0.5 - labelSpace;
      side = 'left';
    } else {
      x = xOff + this.zoom * 0.5 + labelSpace;
      side = 'right';
    }
    for (i = 0; i < data.yLabels.length; ++i) {
      if (data.yLabelType === 'between') {
        y = yOff + this.zoom * util.normalize(i, yNumBuckets);
      } else {
        y = yOff + this.zoom * util.normalize(i + 0.5, yNumBuckets);
      }
      l = new Label(x, y, data.yLabels[i], side);
      yLabels.push(l);
      this.vertices.append(l.dom);
    }
     
    var vertexScale = this.zoom * 2 / util.minmax(data.vertices.map(function(n) { return n.size; })).max;
    for (i = 0; i < data.vertices.length; ++i) {
      var vertex = data.vertices[i];
      var v = new Vertex(xOff + this.zoom * util.normalize(vertex.x + 0.5, xNumBuckets),
                         yOff + this.zoom * util.normalize(vertex.y + 0.5, yNumBuckets),
                         Math.sqrt(vertexScale * vertex.size),
                         vertex.size);
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
      this.edges.append(e.dom);
    }
  };

  function Label(x, y, text, side) {
    var labelClass = 'bucket ' + (side || '');
    this.dom = svg.create('text', {'class': labelClass, x: x, y: y}).text(text);
  }
  Label.prototype.on = function() { svg.addClass(this.dom, 'highlight'); };
  Label.prototype.off = function() { svg.removeClass(this.dom, 'highlight'); };

  function Vertex(x, y, r, text) {
    this.x = x;
    this.y = y;
    this.r = r;
    this.circle = svg.create('circle', {r: r});
    this.label = svg.create('text').text(text);
    this.dom = svg.group([this.circle, this.label], {'class': 'vertex' });
    this.moveListeners = [];
    this.moveTo(x, y);
    this.hoverListeners = [];
    var that = this;
    this.circle.mouseenter(function() {
      svg.addClass(that.dom, 'highlight');
      for (var i = 0; i < that.hoverListeners.length; ++i) {
        that.hoverListeners[i].on(that);
      }
    });
    this.circle.mouseleave(function() {
      svg.removeClass(that.dom, 'highlight');
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
    this.circle.attr({cx: x, cy: y});
    this.label.attr({x: x, y: y});
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
    this.first.attr('d', svg.arrow1(this.src.x, this.src.y, this.dst.x, this.dst.y, zoom));
    this.second.attr('d', svg.arrow2(this.src.x, this.src.y, this.dst.x, this.dst.y, zoom));
  };

  return directive;
});
