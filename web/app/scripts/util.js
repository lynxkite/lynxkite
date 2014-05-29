var SVG = {};

SVG.M = function(x, y) { return ' M ' + x + ' ' + y; }
SVG.A = function(r, x, y) { return ' A ' + r + ' ' + r + ' 0 0 0 ' + x + ' ' + y; }

SVG.arc = function(ax, ay, bx, by) {
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

SVG.arrow1 = function(ax, ay, bx, by) {
  var a = SVG.arc(ax, ay, bx, by);
  return SVG.M(ax, ay) + SVG.A(a.r, a.x, a.y);
}

SVG.arrow2 = function(ax, ay, bx, by) {
  var a = SVG.arc(ax, ay, bx, by);
  return SVG.M(a.x, a.y) + SVG.A(a.r, bx, by);
}

SVG.group = function(l) {
  var g = SVG.create('g');
  g.append(l);
  return g;
}

SVG.arrow = function(ax, ay, bx, by, w) {
  var l1 = SVG.create('path', {'class': 'first', 'd': SVG.arrow1(ax, ay, bx, by), 'stroke-width': w});
  l1.data({x: ax, y: ay});
  var l2 = SVG.create('path', {'class': 'second', 'd': SVG.arrow2(ax, ay, bx, by), 'stroke-width': w});
  l2.data({x: bx, y: by});
  return SVG.group([l1, l2]);
}

SVG.gradient = function(from, to) {
  var id = 'grad-' + btoa(from) + btoa(to);
  var g = SVG.create('radialGradient', {id: id, cx: 0.5, cy: 0.5, r: 0.5, fx: 0.5, fy: 0.5});
  var s1 = SVG.create('stop', {offset: '0%', 'stop-color': from});
  var s2 = SVG.create('stop', {offset: '100%', 'stop-color': to});
  g.append([s1, s2]);
  g.color = 'url(#' + id + ')';
  return g;
}

SVG.hsl = function(h, s, l) {
  return 'hsl(' + Math.floor(h) + ', ' + Math.floor(s) + '%, ' + Math.floor(l) + '%)';
}

SVG.create = function(tag, attrs) {
  var e = angular.element(document.createElementNS('http://www.w3.org/2000/svg', tag));
  if (attrs !== undefined) {
    e.attr(attrs);
  }
  return e;
}

SVG.minmax = function(xs) {
  var Inf = parseFloat('Infinity');
  var min = Inf, max = -Inf;
  for (var i = 0; i < xs.length; ++i) {
    if (xs[i] < min) min = xs[i];
    if (xs[i] > max) max = xs[i];
  }
  return {min: min, max: max, span: max - min};
}

SVG.normalize = function(x, minmax) {
  return (x - minmax.min) / minmax.span - 0.5;
}


