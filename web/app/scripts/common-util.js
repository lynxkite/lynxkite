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

  normalize: function(x, minmax) {
    return (x - minmax.min) / minmax.span - 0.5;
  },
};
