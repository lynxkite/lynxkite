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

  var GRAD = gradient(hsl(0, 50, 70), hsl(0, 50, 10));

  function GraphView(element) {
    var svg = angular.element(element);

    var defs = svg.find('defs');
    defs.append(GRAD);

    this.edges = create('g', {'class': 'edge'});
    this.vertices = create('g', {'class': 'vertex'});
    this.root = svg.find('g.root');
    this.root.append([this.edges, this.vertices]);
  }

  GraphView.prototype.update = function(graph) {
    this.vertices.empty();
    this.edges.empty();
    var vertexScale = 0.01 / minmax(graph.vertices.map(function(n) { return n.count; })).max;
    var xb = minmax(graph.vertices.map(function(n) { return n.x; }));
    var yb = minmax(graph.vertices.map(function(n) { return n.y; }));
    for (var i = 0; i < graph.vertices.length; ++i) {
      var vertex = graph.vertices[i];
      var v = new Vertex(normalize(vertex.x, xb), normalize(vertex.y, yb),
                         Math.sqrt(vertexScale * vertex.count));
      this.vertices.append(v.dom);
    }
    var edgeScale = 0.005 / minmax(graph.vertices.map(function(n) { return n.count; })).max;
    for (i = 0; i < graph.edges.length; ++i) {
      var edge = graph.edges[i];
      var a = graph.vertices[edge.a];
      var b = graph.vertices[edge.b];
      var e = new Edge(normalize(a.x, xb), normalize(a.y, yb),
                       normalize(b.x, xb), normalize(b.y, yb),
                       edgeScale * edge.count);
      this.edges.append(e.dom);
    }
  };

  function Vertex(x, y, r) {
    this.x = x;
    this.y = y;
    this.r = r;
    var c = create('circle', {cx: x, cy: y, r: r, fill: GRAD.color});
    var highlight = create('circle', {cx: x - 0.4 * r, cy: y - 0.4 * r, r: r * 0.1, fill: 'white'});
    this.dom = group([c, highlight]);
  }

  function Edge(x1, y1, x2, y2, w) {
    this.dom = arrow(x1, y1, x2, y2, w);
  }

  function draw() {
    return ' ' + Array.prototype.slice.call(arguments).join(' ') + ' ';
  }

  function arc(r, x, y) { return draw('A', r, r, 0, 0, 0, x, y); }

  function arcParams(ax, ay, bx, by) {
    if (ax === bx && ay === by) {
      return {r: 0.01, x: ax + 0.02, y: ay};
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

  function arrow1(ax, ay, bx, by) {
    var a = arcParams(ax, ay, bx, by);
    return draw('M', ax, ay) + arc(a.r, a.x, a.y);
  }

  function arrow2(ax, ay, bx, by) {
    var a = arcParams(ax, ay, bx, by);
    return draw('M', a.x, a.y) + arc(a.r, bx, by);
  }

  function group(l) {
    var g = create('g');
    g.append(l);
    return g;
  }

  function arrow(ax, ay, bx, by, w) {
    var l1 = create('path', {'class': 'first', 'd': arrow1(ax, ay, bx, by), 'stroke-width': w});
    l1.data({x: ax, y: ay});
    var l2 = create('path', {'class': 'second', 'd': arrow2(ax, ay, bx, by), 'stroke-width': w});
    l2.data({x: bx, y: by});
    return group([l1, l2]);
  }

  function gradient(from, to) {
    var id = 'grad-' + btoa(from) + btoa(to);
    var g = create('radialGradient', {id: id, cx: 0.5, cy: 0.5, r: 0.5, fx: 0.5, fy: 0.5});
    var s1 = create('stop', {offset: '0%', 'stop-color': from});
    var s2 = create('stop', {offset: '100%', 'stop-color': to});
    g.append([s1, s2]);
    g.color = 'url(#' + id + ')';
    return g;
  }

  function hsl(h, s, l) {
    return 'hsl(' + Math.floor(h) + ', ' + Math.floor(s) + '%, ' + Math.floor(l) + '%)';
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
