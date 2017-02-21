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

  return function(data, boxMap) {
    var srcBox = boxMap[data.src.box];
    var dstBox = boxMap[data.dst.box];
    var srcPlug = lookupEndPoint(srcBox.outputs, data.src.id);
    var dstPlug = lookupEndPoint(dstBox.inputs, data.dst.id);

    return {
      x1: srcPlug.x,
      y1: srcPlug.y,
      x2: dstPlug.x,
      y2: dstPlug.y,
    };

  };
});
