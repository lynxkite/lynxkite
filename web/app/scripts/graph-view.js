'use strict';

angular.module('biggraph').directive('graphView', function() {
  /* global SVG_UTIL, COMMON_UTIL */
  var svg = SVG_UTIL;
  var util = COMMON_UTIL;
  var directive = {
      template: '<svg class="graph-view" version="1.1" xmlns="http://www.w3.org/2000/svg"></svg>',
      require: '^ngModel',
      replace: true,
      link: function(scope, element, attrs) {
        var gv = new GraphView(element);
        scope.$watch(attrs.ngModel, function(data) {
          if (data.$resolved) {
            gv.update(data);
          }
        }, true);
      },
    };

  function GraphView(element) {
    var svgtag = angular.element(element);
    svgtag.append([svg.marker('arrow'), svg.marker('arrow-highlight-in'), svg.marker('arrow-highlight-out')]);
    this.edges = svg.create('g', {'class': 'edges'});
    this.vertices = svg.create('g', {'class': 'nodes'});
    this.root = svg.create('g', {'class': 'root'});
    this.zoom = 250;
    this.root.append([this.edges, this.vertices]);
    svgtag.append(this.root);
  }

  GraphView.prototype.update = function(graph) {
    this.vertices.empty();
    this.edges.empty();
    var vertices = [];
    var vertexScale = this.zoom * 2 / util.minmax(graph.vertices.map(function(n) { return n.count; })).max;
    var xb = util.minmax(graph.vertices.map(function(n) { return n.x; }));
    var yb = util.minmax(graph.vertices.map(function(n) { return n.y; }));
    for (var i = 0; i < graph.vertices.length; ++i) {
      var vertex = graph.vertices[i];
      var v = new Vertex(this.zoom * util.normalize(vertex.x, xb), this.zoom * util.normalize(vertex.y, yb),
                         Math.sqrt(vertexScale * vertex.count),
                         vertex.count);
      vertices.push(v);
      if (vertex.count === 0) {
        continue;
      }
      this.vertices.append(v.dom);
    }
    var edgeScale = this.zoom * 0.05 / util.minmax(graph.edges.map(function(n) { return n.count; })).max;
    for (i = 0; i < graph.edges.length; ++i) {
      var edge = graph.edges[i];
      if (edgeScale * edge.count < 1) {
        continue;
      }
      var a = vertices[edge.a];
      var b = vertices[edge.b];
      var e = new Edge(a, b, edgeScale * edge.count, this.zoom);
      this.edges.append(e.dom);
    }
  };

  function Vertex(x, y, r, text) {
    this.x = x;
    this.y = y;
    this.r = r;
    this.circle = svg.create('circle', {r: r, 'class': 'vertex'});
    this.label = svg.create('text', {text: text, 'class': 'vertex-label'});
    this.label.text(text);
    this.dom = svg.group([this.circle, this.label]);
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
    dst.addHoverListener({on: function() { svg.addClass(that.dom, 'highlight-in'); that.toFront(); },
                          off: function() { svg.removeClass(that.dom, 'highlight-in'); }});
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
