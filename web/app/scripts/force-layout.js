'use strict';

/* exported FORCE_LAYOUT */
var FORCE_LAYOUT = (function() {
  var lib = {};
  lib.Engine = function(opts) {
    this.opts = opts;
  };
  lib.Engine.prototype.step = function(vertices) {
    var max_dist = 0;
    for (var i = 0; i < vertices.edges.length; ++i) {
      var e = vertices.edges[i];
      if (e.size === 0) { continue; }
      var a = e.src, b = e.dst;
      a.x += this.opts.attraction * (b.force_ox - a.force_ox) / a.force_mass;
      a.y += this.opts.attraction * (b.force_oy - a.force_oy) / a.force_mass;
      b.x += this.opts.attraction * (a.force_ox - b.force_ox) / b.force_mass;
      b.y += this.opts.attraction * (a.force_oy - b.force_oy) / b.force_mass;
    }
    for (i = 0; i < vertices.length; ++i) {
      var a = vertices[i];
      if (a.count === 0) { continue; }
      var dx = a.force_ox - vertices.xOff;
      var dy = a.force_oy - vertices.yOff;
      a.x -= this.opts.gravity * dx;
      a.y -= this.opts.gravity * dy;
      if (max_dist < Math.abs(dx)) { max_dist = Math.abs(dx); }
      if (max_dist < Math.abs(dy)) { max_dist = Math.abs(dy); }
      for (var j = 0; j < vertices.length; ++j) {
        var b = vertices[j];
        var dx = a.force_ox - b.force_ox;
        var dy = a.force_oy - b.force_oy;
        if (dx === 0 && dy === 0) {
          dx = Math.random();
          dy = Math.random();
        }
        var d2 = dx * dx + dy * dy;
        a.x += this.opts.repulsion * dx / d2 / a.force_mass;
        a.y += this.opts.repulsion * dy / d2 / a.force_mass;
        b.x -= this.opts.repulsion * dx / d2 / b.force_mass;
        b.y -= this.opts.repulsion * dy / d2 / b.force_mass;
      }
    }
    var total_change = 0;
    for (i = 0; i < vertices.length; ++i) {
      var v = vertices[i];
      var oox = v.force_ox, ooy = v.force_oy;
      v.force_ox = v.x; v.force_oy = v.y;
      v.x += (1 - this.opts.drag) * (v.force_ox - oox);
      v.y += (1 - this.opts.drag) * (v.force_oy - ooy);
      v.moveTo(v.x, v.y);
      total_change += Math.abs(v.x - oox) + Math.abs(v.y - ooy);
    }
    return 0.001 < total_change / vertices.length / max_dist;
  };
  return lib;
}());
