'use strict';

/* exported COMMON_UTIL */
var COMMON_UTIL = {
  minmax: function(xs) {
    var Inf = parseFloat('Infinity');
    var min = Inf, max = -Inf;
    for (var i = 0; i < xs.length; ++i) {
      if (xs[i] < min) { min = xs[i]; }
      if (xs[i] > max) { max = xs[i]; }
    }
    return {min: min, max: max, span: max - min};
  },

  // minmax for dynamicValue
  dynMinmax: function(xs) {
    var Inf = parseFloat('Infinity');
    var min = Inf, max = -Inf;
    var minIdx, maxIdx;
    for (var i = 0; i < xs.length; ++i) {
      if (xs[i].double < min) { min = xs[i].double; minIdx = i; }
      if (xs[i].double > max) { max = xs[i].double; maxIdx = i; }
    }
    return {min: xs[minIdx], max: xs[maxIdx], span: max - min};
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
};
