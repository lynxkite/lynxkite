'use strict';

angular.module('biggraph').directive('graphView', function($window) {
  /* global SVG_UTIL, COMMON_UTIL */
  var svg = SVG_UTIL;
  var util = COMMON_UTIL;
  var directive = {
      template: '<svg class="graph-view" version="1.1" xmlns="http://www.w3.org/2000/svg"></svg>',
      scope: { ngModel: '=' },
      replace: true,
      link: function(scope, element) {
        var gv = new GraphView(element);
        function updateGraph() {
          if (scope.ngModel !== undefined && scope.ngModel.$resolved) {
            gv.update(scope.ngModel);
          }
        }
        scope.$watch('ngModel', updateGraph, true);
        angular.element($window).bind('resize', updateGraph);
      },
    };

  function GraphView(element) {
    this.svg = angular.element(element);
    this.svg.append([svg.marker('arrow'), svg.marker('arrow-highlight-in'), svg.marker('arrow-highlight-out')]);
    this.edges = svg.create('g', {'class': 'edges'});
    this.vertices = svg.create('g', {'class': 'nodes'});
    this.root = svg.create('g', {'class': 'root'});
    this.zoom = 250;
    this.root.append([this.edges, this.vertices]);
    this.svg.append(this.root);
  }

  GraphView.prototype.update = function(data) {
    this.vertices.empty();
    this.edges.empty();
    var vertices = [];
    var n = data.vertexSets.length;
    for (var i = 0; i < n; ++i) {
      var xOff = (i * 2 + 1) * this.svg.width() / n / 2;
      var yOff = 250;
      vertices.push(this.addVertices(data.vertexSets[i], xOff, yOff));
    }
    for (i = 0; i < data.edgeBundles.length; ++i) {
      var e = data.edgeBundles[i];
      this.addEdges(e.edges, vertices[e.srcs], vertices[e.dsts]);
    }
  };

  GraphView.prototype.addVertices = function(data, xOff, yOff) {
    var vertices = [];
    var vertexScale = this.zoom * 2 / util.minmax(data.vertices.map(function(n) { return n.count; })).max;
    var xb = util.minmax(data.vertices.map(function(n) { return n.x; }));
    var yb = util.minmax(data.vertices.map(function(n) { return n.y; }));
    var xBuckets = [], yBuckets = [];
    var i, x, y, l;
    y = yOff + this.zoom * util.normalize(yb.max + 1, yb);
    for (i = 0; i < data.xBuckets.length; ++i) {
      x = xOff + this.zoom * util.normalize(i, xb);
      l = new Label(x, y, data.xBuckets[i]);
      xBuckets.push(l);
      this.vertices.append(l.dom);
    }
    // Labels on the left on the left and on the right on the right.
    var pos = xOff < this.svg.width() / 2 ? xb.min - 1 : xb.max + 1;
    x = xOff + this.zoom * util.normalize(pos, xb);
    for (i = 0; i < data.yBuckets.length; ++i) {
      y = yOff + this.zoom * util.normalize(i, yb);
      l = new Label(x, y, data.yBuckets[i]);
      yBuckets.push(l);
      this.vertices.append(l.dom);
    }
    for (i = 0; i < data.vertices.length; ++i) {
      var vertex = data.vertices[i];
      var v = new Vertex(xOff + this.zoom * util.normalize(vertex.x, xb),
                         yOff + this.zoom * util.normalize(vertex.y, yb),
                         Math.sqrt(vertexScale * vertex.count),
                         vertex.count);
      vertices.push(v);
      if (vertex.count === 0) {
        continue;
      }
      this.vertices.append(v.dom);
      if (xBuckets.length !== 0) { v.addHoverListener(xBuckets[vertex.x]); }
      if (yBuckets.length !== 0) { v.addHoverListener(yBuckets[vertex.y]); }
    }
    return vertices;
  };

  GraphView.prototype.addEdges = function(edges, srcs, dsts) {
    var edgeScale = this.zoom * 0.05 / util.minmax(edges.map(function(n) { return n.count; })).max;
    for (var i = 0; i < edges.length; ++i) {
      var edge = edges[i];
      if (edgeScale * edge.count < 0.1) {
        continue;
      }
      var a = srcs[edge.a];
      var b = dsts[edge.b];
      var e = new Edge(a, b, edgeScale * edge.count, this.zoom);
      this.edges.append(e.dom);
    }
  };

  function Label(x, y, text) {
    this.dom = svg.create('text', {'class': 'bucket', x: x, y: y}).text(text);
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
