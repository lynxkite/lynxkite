'use strict';

angular.module('biggraph').factory('createBox', function() {
  return function(box) {
    var width = 200;
    var height = 40;
    var plugRadius = 8;

    function createPlug(plug, index, direction) {
      var len = box[direction].length;
      var x;
      if (len <= 1) {
        x = width / 2;
      } else {
        var r = plugRadius;
        x = (width - r) / len * index + r;
      }
      var y = direction === 'outputs' ? height + 15 : -15;

      return {
        boxId: box.id,
        data: plug,
        index: index,
        direction: direction,
        radius: plugRadius,
        x: function() { return x + box.x; },
        y: function() { return y + box.y; },
        toArrowEnd: function() {
          return {
            box: this.boxId,
            id: this.data.id,
            kind: this.data.kind
          };
        },
        posTransform: 'translate(' + x + ', ' + y + ')'
      };
    }

    var inputs = [];
    var outputs = [];
    var i;
    for (i = 0; i < box.inputs.length; ++i) {
      inputs.push(createPlug(box.inputs[i], i, 'inputs'));
    }
    for (i = 0; i < box.outputs.length; ++i) {
      outputs.push(createPlug(box.outputs[i], i, 'outputs'));
    }

    return {
      data: box,
      width: width,
      height: height,
      plugRadius: plugRadius,
      operation: box.operation,
      mainPosTransform: function() {
        return 'translate(' + this.data.x + ', ' + this.data.y + ')';
      },
      inputs: inputs,
      outputs: outputs,
      getPlugName: function(index, direction) {
        return this.data[direction][index].id;
      },
      onMouseMove: function(event) {
        this.data.x = event.clientX + this.xOffset;
        this.data.y = event.clientY + this.yOffset;
      },
      onMouseDown: function(event) {
        this.xOffset = this.data.x - event.clientX;
        this.yOffset = this.data.y - event.clientY;
      },
    };
  };
});
