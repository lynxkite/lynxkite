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

angular.module('biggraph').factory('boxWrapper', function(plugWrapper) {
  return function(metadata, instance) {
    function getCommentLines() {
      var comment;
      if (metadata.operationID === 'Add comment') {
        comment = instance.parameters.comment;
      } else if (metadata.operationID === 'Anchor') {
        comment = instance.parameters.description;
      }
      return comment ? comment.split('\n') : [];
    }

    var box = {
      metadata: metadata,
      instance: instance,
      inputs: [],
      outputs: [],
      outputMap: {},
      width: 200,
      height: 40,
      commentLines: getCommentLines(),
      isMoved: false,
      mainPosTransform: function() {
        return 'translate(' + this.instance.x + ', ' + this.instance.y + ')';
      },
      onMouseMove: function(event) {
        this.isMoved = true;
        this.instance.x = event.logicalX + this.xOffset;
        this.instance.y = event.logicalY + this.yOffset;
      },
      onMouseDown: function(event) {
        this.xOffset = this.instance.x - event.logicalX;
        this.yOffset = this.instance.y - event.logicalY;
      },
    };

    var i;
    for (i = 0; i < box.metadata.inputs.length; ++i) {
      box.inputs.push(plugWrapper(box.metadata.inputs[i], i, 'inputs', box));
    }
    for (i = 0; i < box.metadata.outputs.length; ++i) {
      var plug = plugWrapper(box.metadata.outputs[i], i, 'outputs', box);
      box.outputs.push(plug);
      box.outputMap[plug.id] = plug;
    }

    return box;
  };
});
