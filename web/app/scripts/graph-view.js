'use strict';

angular.module('biggraph').directive('graphView', function() {
  var directive = {
      template: '<svg class="graph-view" version="1.1" xmlns="http://www.w3.org/2000/svg"><defs></defs><g class="root"></g></svg>',
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
    var svg = angular.element(element);
    this.edges = create('g');
    this.vertices = create('g');
    this.root = svg.find('g.root');
    this.zoom = 250;
    this.root.append([this.edges, this.vertices]);
  }

  GraphView.prototype.update = function(graph) {
    this.vertices.empty();
    this.edges.empty();
    var vertices = [];
    var vertexScale = this.zoom * 2 / minmax(graph.vertices.map(function(n) { return n.count; })).max;
    var xb = minmax(graph.vertices.map(function(n) { return n.x; }));
    var yb = minmax(graph.vertices.map(function(n) { return n.y; }));
    for (var i = 0; i < graph.vertices.length; ++i) {
      var vertex = graph.vertices[i];
      var v = new Vertex(this.zoom * normalize(vertex.x, xb), this.zoom * normalize(vertex.y, yb),
                         Math.sqrt(vertexScale * vertex.count),
                         vertex.count);
      vertices.push(v);
      this.vertices.append(v.dom);
    }
    var edgeScale = this.zoom * 0.005 / minmax(graph.vertices.map(function(n) { return n.count; })).max;
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
    this.circle = create('circle', {r: r, 'class': 'vertex'});
    this.label = create('text', {text: text, 'class': 'vertex-label'});
    this.label.text(text);
    this.dom = group([this.circle, this.label]);
    this.moveListeners = [];
    this.moveTo(x, y);
  }
  Vertex.prototype.addMoveListener = function(ml) {
    this.moveListeners.push(ml);
    ml(this);
  }
  Vertex.prototype.moveTo = function(x, y) {
    this.x = x;
    this.y = y;
    this.circle.attr({cx: x, cy: y});
    this.label.attr({x: x, y: y});
    for (var i = 0; i < this.moveListeners.length; ++i) {
      this.moveListeners[i](this);
    }
  }

  function Edge(src, dst, w, zoom) {
    this.src = src;
    this.dst = dst;
    this.first = create('path', {'class': 'first', 'stroke-width': w});
    this.second = create('path', {'class': 'second', 'stroke-width': w});
    this.dom = group([this.first, this.second], {'class': 'edge'});
    var that = this
    src.addMoveListener(function() { that.reposition(zoom); });
    dst.addMoveListener(function() { that.reposition(zoom); });
  }
  Edge.prototype.reposition = function(zoom) {
    this.first.attr('d', arrow1(this.src.x, this.src.y, this.dst.x, this.dst.y, zoom));
    this.second.attr('d', arrow2(this.src.x, this.src.y, this.dst.x, this.dst.y, zoom));
  }

  function draw() {
    return ' ' + Array.prototype.slice.call(arguments).join(' ') + ' ';
  }

  function arc(r, x, y) { return draw('A', r, r, 0, 0, 0, x, y); }

  function arcParams(ax, ay, bx, by, zoom) {
    if (ax === bx && ay === by) {
      return {r: 0.1 * zoom, x: ax + 0.2 * zoom, y: ay};
    } else {
      var dx = bx - ax, dy = by - ay;
      var h = 1 - Math.sqrt(3) / 2;
      return {
        r: Math.sqrt(dx * dx + dy * dy),
        x: ax + 0.5 * dx - h * dy,
        y: ay + 0.5 * dy + h * dx,
      };
    }
  }

  function arrow1(ax, ay, bx, by, zoom) {
    var a = arcParams(ax, ay, bx, by, zoom);
    return draw('M', ax, ay) + arc(a.r, a.x, a.y);
  }

  function arrow2(ax, ay, bx, by, zoom) {
    var a = arcParams(ax, ay, bx, by, zoom);
    return draw('M', a.x, a.y) + arc(a.r, bx, by);
  }

  function group(l, attrs) {
    var g = create('g', attrs);
    g.append(l);
    return g;
  }

  function create(tag, attrs) {
    var e = angular.element(document.createElementNS('http://www.w3.org/2000/svg', tag));
    if (attrs !== undefined) {
      e.attr(attrs);
    }
    return e;
  }

  function minmax(xs) {
    var Inf = parseFloat('Infinity');
    var min = Inf, max = -Inf;
    for (var i = 0; i < xs.length; ++i) {
      if (xs[i] < min) { min = xs[i]; }
      if (xs[i] > max) { max = xs[i]; }
    }
    return {min: min, max: max, span: max - min};
  }

  function normalize(x, minmax) {
    return (x - minmax.min) / minmax.span - 0.5;
  }

  return directive;
});
