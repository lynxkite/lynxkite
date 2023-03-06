// Assorted utility functions.
'use strict';

/* eslint-disable no-unused-vars */
export default {
  minmax: function(xs) {
    const Inf = parseFloat('Infinity');
    let min = Inf, max = -Inf;
    for (let i = 0; i < xs.length; ++i) {
      if (xs[i] < min) { min = xs[i]; }
      if (xs[i] > max) { max = xs[i]; }
    }
    return {min: min, max: max, span: max - min};
  },

  // normalization between [-0.5,0.5]
  normalize: function(x, minmax) {
    if (minmax.span) {
      // normalize minmax object
      if (minmax.span > 0) {
        return (x - minmax.min) / minmax.span - 0.5;
      } else {
        return 0.0;
      }
    } else {
      // normalize single number
      if (minmax > 0) {
        return x / minmax - 0.5;
      } else {
        return 0.0;
      }
    }
  },

  // Java's hash function in JS.
  hashCode: function(text) {
    /* eslint-disable no-bitwise */
    let hash = 0;
    for (let i = 0; i < text.length; ++i) {
      hash = hash * 31 + text.charCodeAt(i);
      hash |= 0; // Trim to 32 bits.
    }
    return Math.abs(hash);
  }
};
