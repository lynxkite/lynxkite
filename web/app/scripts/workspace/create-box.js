'use strict';

// Creates a box object for a workspace.
// This object wraps the actual box data representation and
// provides methods to interact with the GUI, e.g. mouse events
// and values to bind with Angular to SVG elements.
// The wrapped box date representation has two parts:
// - metadata describes the type of operation of the box.
// - instance describes the operation parameter values,
//   coordinates on the workspace and everything related to this
//   box instance that have to be saved.

angular.module('biggraph').factory('createBox', function() {
  return function(metadata, instance) {
    var width = instance.width;
    var height = instance.height;

    // An input or output connection point of a box.
    function createPlug(plug, index, direction) {
      var plugRadius = 8;

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
      inputs: inputs,
      outputs: outputs,
      width: width,
      height: height,
      isMoved: false,
      mainPosTransform: function() {
        return 'translate(' + this.instance.x + ', ' + this.instance.y + ')';
      },
      onMouseMove: function(event) {
        this.isMoved = true;
        this.instance.x = event.clientX + this.xOffset;
        this.instance.y = event.clientY + this.yOffset;
      },
      onMouseDown: function(event) {
        this.xOffset = this.instance.x - event.clientX;
        this.yOffset = this.instance.y - event.clientY;
      },
      isCommentBox: function() {
        return this.metadata.operationID === 'Add comment';
        }
    };
  };
});
