'use strict';

/* exported FORCE_LAYOUT */
var FORCE_LAYOUT = (function() {
  var lib = {};
  // Options:
  //   attraction:      force/distance ratio along edges
  //   repulsion:       force absolute value between all vertices
  //   gravity:         force/distance from origin ratio
  //   drag:            force/speed ratio (fluid resistance)
  //   labelAttraction: attraction between matching labels, as fraction of repulsion
  //   centralize:      whether to ...
  lib.Engine = function(opts) {
    this.opts = opts;
  };
  lib.Engine.prototype.step = function(vertices) {
    var changed = this.calculate(vertices);
    this.apply(vertices);
    return changed;
  };
  lib.Engine.prototype.calculate = function(vertices) {
    var a, b, dx, dy, i, j;
    var maxDist = 0;
    if (vertices.edges !== undefined) {
      for (i = 0; i < vertices.edges.length; ++i) {
        var e = vertices.edges[i];
        if (e.size === 0) { continue; }
        a = e.src;
        b = e.dst;
        a.x += this.opts.attraction * (b.forceOX - a.forceOX) / a.forceMass;
        a.y += this.opts.attraction * (b.forceOY - a.forceOY) / a.forceMass;
        b.x += this.opts.attraction * (a.forceOX - b.forceOX) / b.forceMass;
        b.y += this.opts.attraction * (a.forceOY - b.forceOY) / b.forceMass;
      }
    }
    for (i = 0; i < vertices.length; ++i) {
      a = vertices[i];
      if (a.count === 0) { continue; }
      dx = a.forceOX;
      dy = a.forceOY;
      a.x -= this.opts.gravity * dx;
      a.y -= this.opts.gravity * dy;
      if (maxDist < Math.abs(dx)) { maxDist = Math.abs(dx); }
      if (maxDist < Math.abs(dy)) { maxDist = Math.abs(dy); }
      for (j = 0; j < vertices.length; ++j) {
        if (i === j) { continue; }
        b = vertices[j];
        dx = a.forceOX - b.forceOX;
        dy = a.forceOY - b.forceOY;
        if (dx === 0 && dy === 0) {
          dx = Math.random();
          dy = Math.random();
        }
        var d2 = dx * dx + dy * dy;
        var repulsion = this.opts.repulsion;
        if (a.text !== undefined && b.text !== undefined && a.text === b.text) {
          // Apply reduced repulsion between vertices that have the same label.
          // This causes the vertices to cluster a bit by label.
          repulsion *= 1.0 - this.opts.labelAttraction;
        }
        if (this.opts.centralize) {
          a.x += repulsion * dx / d2 / a.forceMass;
          a.y += repulsion * dy / d2 / a.forceMass;
          b.x -= repulsion * dx / d2 / b.forceMass;
          b.y -= repulsion * dy / d2 / b.forceMass;
        } else {
          a.x += repulsion * dx / d2 * a.forceMass;
          a.y += repulsion * dy / d2 * a.forceMass;
          b.x -= repulsion * dx / d2 * b.forceMass;
          b.y -= repulsion * dy / d2 * b.forceMass;
        }
      }
    }
    var totalChange = 0;
    for (i = 0; i < vertices.length; ++i) {
      var v = vertices[i];
      if (v.dragged || v.frozen || v.positioned) {
        // Cancel movement.
        v.x = v.forceOX;
        v.y = v.forceOY;
      }
      var oox = v.forceOX, ooy = v.forceOY;
      v.forceOX = v.x; v.forceOY = v.y;
      v.x += (1 - this.opts.drag) * (v.forceOX - oox);
      v.y += (1 - this.opts.drag) * (v.forceOY - ooy);
      totalChange += Math.abs(v.x - oox) + Math.abs(v.y - ooy);
    }
    return 0.0001 < totalChange / vertices.length / maxDist;
  };
  lib.Engine.prototype.apply = function(vertices) {
    for (var i = 0; i < vertices.length; ++i) {
      var v = vertices[i];
      v.moveTo(v.x, v.y);
    }
  };
  return lib;
}());
