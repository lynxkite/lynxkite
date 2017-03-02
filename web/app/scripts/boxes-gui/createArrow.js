'use strict';

angular.module('biggraph').factory('createArrow', function() {

  function lookupEndPoint(list, id) {
    for (var i = 0; i < list.length; ++i) {
      if (list[i].data.id === id) {
        return list[i];
      }
    }
    return undefined;
  }

  return function(srcList, srcId, dstList, dstId) {
    var srcPlug = lookupEndPoint(srcList, srcId);
    var dstPlug = lookupEndPoint(dstList, dstId);

    return {
      x1: srcPlug.x,
      y1: srcPlug.y,
      x2: dstPlug.x,
      y2: dstPlug.y,
    };

  };
});
