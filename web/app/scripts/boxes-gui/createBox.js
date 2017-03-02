'use strict';

angular.module('biggraph').factory('createBox', function() {
  return function(metadata, instance) {
    var width = 200;
    var height = 40;
    var plugRadius = 8;

    function createPlug(plug, index, direction) {
      var len = metadata[direction].length;
      var x;
      if (len <= 1) {
        x = width / 2;
      } else {
        var r = plugRadius;
        x = (width - r) / len * index + r;
      }
      var y = direction === 'outputs' ? height + 15 : -15;

      return {
        boxId: instance.id,
        instance: instance,
        data: plug,
        index: index,
        direction: direction,
        radius: plugRadius,
        x: function() { return x + instance.x; },
        y: function() { return y + instance.y; },
/*        toArrowEnd: function() {
          return {
            box: this.boxId,
            id: this.data.id,
            kind: this.data.kind
          };
        },*/
        posTransform: 'translate(' + x + ', ' + y + ')'
      };
    }

    var inputs = [];
    var outputs = [];
    var i;
    for (i = 0; i < metadata.inputs.length; ++i) {
      inputs.push(createPlug(metadata.inputs[i], i, 'inputs'));
    }
    for (i = 0; i < metadata.outputs.length; ++i) {
      outputs.push(createPlug(metadata.outputs[i], i, 'outputs'));
    }
    return {
      metadata: metadata,
      instance: instance,

      width: width,
      height: height,
      plugRadius: plugRadius,
      mainPosTransform: function() {
        return 'translate(' + this.instance.x + ', ' + this.instance.y + ')';
      },
      inputs: inputs,
      outputs: outputs,
      getPlugName: function(index, direction) {
        return this.data[direction][index].id;
      },
      onMouseMove: function(event) {
        this.instance.x = event.clientX + this.xOffset;
        this.instance.y = event.clientY + this.yOffset;
      },
      onMouseDown: function(event) {
        this.xOffset = this.instance.x - event.clientX;
        this.yOffset = this.instance.y - event.clientY;
      },
    };
  };
});
