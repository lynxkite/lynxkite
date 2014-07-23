'use strict';

/* exported FORCE_LAYOUT */
var FORCE_LAYOUT = (function() {
  var lib = {};
  // Options:
  //   attraction: force/distance ratio along edges
  //   repulsion:  force absolute value between all vertices
  //   gravity:    force/distance from origin ratio
  //   drag:       force/speed ratio (fluid resistance)
  lib.Engine = function(opts) {
    this.opts = opts;
  };
  lib.Engine.prototype.step = function(vertices) {
    var a, b, dx, dy, i, j;
    var maxDist = 0;
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
    for (i = 0; i < vertices.length; ++i) {
      a = vertices[i];
      if (a.count === 0) { continue; }
      dx = a.forceOX - vertices.xOff;
      dy = a.forceOY - vertices.yOff;
      a.x -= this.opts.gravity * dx;
      a.y -= this.opts.gravity * dy;
      if (maxDist < Math.abs(dx)) { maxDist = Math.abs(dx); }
      if (maxDist < Math.abs(dy)) { maxDist = Math.abs(dy); }
      for (j = 0; j < vertices.length; ++j) {
        b = vertices[j];
        dx = a.forceOX - b.forceOX;
        dy = a.forceOY - b.forceOY;
        if (dx === 0 && dy === 0) {
          dx = Math.random();
          dy = Math.random();
        }
        var d2 = dx * dx + dy * dy;
        a.x += this.opts.repulsion * dx / d2 / a.forceMass;
        a.y += this.opts.repulsion * dy / d2 / a.forceMass;
        b.x -= this.opts.repulsion * dx / d2 / b.forceMass;
        b.y -= this.opts.repulsion * dy / d2 / b.forceMass;
      }
    }
    var totalChange = 0;
    for (i = 0; i < vertices.length; ++i) {
      var v = vertices[i];
      if (v.dragged) {
        // Cancel movement.
        v.x = v.forceOX;
        v.y = v.forceOY;
      }
      var oox = v.forceOX, ooy = v.forceOY;
      v.forceOX = v.x; v.forceOY = v.y;
      v.x += (1 - this.opts.drag) * (v.forceOX - oox);
      v.y += (1 - this.opts.drag) * (v.forceOY - ooy);
      v.moveTo(v.x, v.y);
      totalChange += Math.abs(v.x - oox) + Math.abs(v.y - ooy);
    }
    return 0.0001 < totalChange / vertices.length / maxDist;
  };
  return lib;
}());
