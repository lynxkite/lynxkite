// The 2D graph layout algoritm used in sampled mode.
'use strict';

/* eslint-disable no-unused-vars */
var FORCE_LAYOUT = (function() {
  var lib = {};
  // Options:
  //   attraction:      force/distance ratio along edges
  //   repulsion:       force absolute value between all vertices
  //   gravity:         force/distance from origin ratio
  //   drag:            force/speed ratio (fluid resistance)
  //   labelAttraction: attraction between matching labels, as fraction of repulsion
  //   style:           specifics of the force algorithm
  lib.Engine = function(opts) {
    this.opts = opts;
  };
  lib.Engine.prototype.step = function(vertices) {
    var changed = this.calculate(vertices);
    this.apply(vertices);
    return changed;
  };
  lib.Engine.prototype.calculate = function(vertices) {
    var a, b, dx, dy, i, j, v;
    var maxDist = 0;  // Max. distance from center along one coordinate.
    var edgeCount = (vertices.edges || []).length;
    if (vertices.vs.length <= 1) {
      if (vertices.vs.length === 1 && !vertices.vs[0].positioned) {
        vertices.vs[0].x = 0;
        vertices.vs[0].y = 0;
      }
      return false;  // No need for further iterations.
    }
    for (i = 0; i < vertices.vs.length; ++i) {
      v = vertices.vs[i];
      if (this.opts.style === 'decentralize') {
        // Higher-degree vertices are lighter, so they get pushed to the periphery.
        v.forceMass = vertices.vs.length / (v.degree + 1);
      } else if (this.opts.style === 'neutral') {
        // All vertices have the same weight, so graph structure dominates the layout.
        // (Make the total mass match the "centralize" mode, so that the layout is
        // of a similar size.)
        v.forceMass = (2.0 * edgeCount + vertices.vs.length) / vertices.vs.length;
      } else /* this.opts.style === 'centralize' */ {
        // Higher-degree vertices are heavier, so they fall into the center.
        v.forceMass = v.degree + 1;
      }
    }
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
    for (i = 0; i < vertices.vs.length; ++i) {
      a = vertices.vs[i];
      if (a.count === 0) { continue; }
      dx = a.forceOX;
      dy = a.forceOY;
      a.x -= this.opts.gravity * dx;
      a.y -= this.opts.gravity * dy;
      if (maxDist < Math.abs(dx)) { maxDist = Math.abs(dx); }
      if (maxDist < Math.abs(dy)) { maxDist = Math.abs(dy); }
      for (j = 0; j < vertices.vs.length; ++j) {
        if (i === j) { continue; }
        b = vertices.vs[j];
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
        a.x += repulsion * dx / d2 / a.forceMass;
        a.y += repulsion * dy / d2 / a.forceMass;
        b.x -= repulsion * dx / d2 / b.forceMass;
        b.y -= repulsion * dy / d2 / b.forceMass;
      }
    }
    var totalChange = 0;
    for (i = 0; i < vertices.vs.length; ++i) {
      v = vertices.vs[i];
      if (v.held || v.frozen || v.positioned) {
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
    return 0.0001 < totalChange / vertices.vs.length / maxDist;
  };
  lib.Engine.prototype.apply = function(vertices) {
    for (var i = 0; i < vertices.vs.length; ++i) {
      var v = vertices.vs[i];
      v.moveTo(v.x, v.y);
    }
  };
  return lib;
}());
