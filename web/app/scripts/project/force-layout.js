// The 2D graph layout algoritm used in sampled mode.
'use strict';

/* eslint-disable no-unused-vars */
const lib = {};
// Options:
//   attraction:      force/distance ratio along edges
//   repulsion:       force absolute value between all vertices
//   gravity:         force/distance from origin ratio
//   drag:            force/speed ratio (fluid resistance)
//   labelAttraction: attraction between matching labels, as fraction of repulsion
//   style:           specifics of the force algorithm
//   componentRepulsionFraction: fraction of normal repulsion to apply between separate components
//   repulsionPower:  the power of distance at which repulsion falls of
lib.Engine = function(opts) {
  this.opts = opts;
};
lib.Engine.prototype.step = function(vertices) {
  const changed = this.calculate(vertices);
  this.apply(vertices);
  return changed;
};
lib.Engine.prototype.calculate = function(vertices) {
  let a, b, dx, dy, i, j, v;
  let maxDist = 0; // Max. distance from center along one coordinate.
  const edgeCount = (vertices.edges || []).length;
  if (vertices.vs.length <= 1) {
    if (vertices.vs.length === 1 && !vertices.vs[0].frozen) {
      vertices.vs[0].x = 0;
      vertices.vs[0].y = 0;
    }
    return false; // No need for further iterations.
  }
  for (i = 0; i < vertices.vs.length; ++i) {
    v = vertices.vs[i];
    if (this.opts.style === 'decentralize') {
      // Higher-degree vertices are lighter, so they get pushed to the periphery.
      v.forceMass = vertices.vs.length / (v.degree + 1);
    } else if (this.opts.style === 'centralize') {
      // Higher-degree vertices are heavier, so they fall into the center.
      v.forceMass = v.degree + 1;
    } else {
      // All vertices have the same weight, so graph structure dominates the layout.
      // (Make the total mass match the "centralize" mode, so that the layout is
      // of a similar size.)
      v.forceMass = (2.0 * edgeCount + vertices.vs.length) / vertices.vs.length;
    }
  }
  if (vertices.edges !== undefined) {
    for (i = 0; i < vertices.edges.length; ++i) {
      const e = vertices.edges[i];
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
      const dxp = Math.abs(Math.pow(dx, this.opts.repulsionPower));
      const dyp = Math.abs(Math.pow(dy, this.opts.repulsionPower));
      const dp = Math.max(1, dxp, dyp);
      let repulsion = this.opts.repulsion;
      if (a.text !== undefined && b.text !== undefined && a.text === b.text) {
        // Apply reduced repulsion between vertices that have the same label.
        // This causes the vertices to cluster a bit by label.
        repulsion *= 1.0 - this.opts.labelAttraction;
      }
      if (a.component !== b.component) {
        repulsion *= this.opts.componentRepulsionFraction;
      }
      a.x += repulsion * dx / dp / a.forceMass;
      a.y += repulsion * dy / dp / a.forceMass;
      b.x -= repulsion * dx / dp / b.forceMass;
      b.y -= repulsion * dy / dp / b.forceMass;
    }
  }
  let totalChange = 0;
  for (i = 0; i < vertices.vs.length; ++i) {
    v = vertices.vs[i];
    if (v.held || v.frozen) {
      // Cancel movement.
      v.x = v.forceOX;
      v.y = v.forceOY;
    }
    const oox = v.forceOX, ooy = v.forceOY;
    v.forceOX = v.x; v.forceOY = v.y;
    v.x += (1 - this.opts.drag) * (v.forceOX - oox);
    v.y += (1 - this.opts.drag) * (v.forceOY - ooy);
    totalChange += Math.abs(v.x - oox) + Math.abs(v.y - ooy);
  }
  return 0.001 < totalChange / vertices.vs.length / maxDist;
};

lib.Engine.prototype.apply = function(vertices) {
  for (let i = 0; i < vertices.vs.length; ++i) {
    const v = vertices.vs[i];
    v.moveTo(v.x, v.y);
  }
};

// Runs the simulation until it stabilizes or the timeout is hit.
lib.Engine.prototype.initForSeconds = function(vertices, seconds) {
  const t0 = Date.now();
  /* eslint-disable no-empty */
  while (this.calculate(vertices) && Date.now() - t0 <= seconds * 1000) {}
  this.apply(vertices);
};

export default lib;
