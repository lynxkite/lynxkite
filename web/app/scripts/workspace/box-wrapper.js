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

angular.module('biggraph').factory('boxWrapper', function() {
  return function(metadata, instance) {
    var width = 200;
    var height = 40;

    // An input or output connection point of a box.
    function plugWrapper(id, index, direction) {
      var radius = 8;

      var len = metadata[direction].length;
      var x;
      if (len <= 1) {
        x = width / 2;
      } else {
        x = index * (width - radius * 2) / (len - 1) + radius;
      }
      var y = direction === 'outputs' ? height + 15 : -15;

      function progressToColor(progressRatio) {
        /* global tinycolor */
        return tinycolor.mix('blue', 'green', progressRatio * 100).toHexString();
      }

      return {
        boxId: instance.id,
        boxInstance: instance,
        id: id,
        direction: direction,
        radius: radius,
        x: function() { return x + instance.x; },
        y: function() { return y + instance.y; },
        posTransform: 'translate(' + x + ', ' + y + ')',
        inProgress: false,
        progressColor: undefined,
        error: '',
        updateProgress: function(progress) {
          var all = 0;
          for (var p in progress) {
            if (progress.hasOwnProperty(p)) {
              all += progress[p];
            }
          }
          if (all) {
            var progressPercentage = progress.computed / all;
            this.progressColor = progressToColor(progressPercentage);
            this.inProgress = progress.inProgress > 0;
          } else {
            this.clearProgress();
          }
        },

        clearProgress: function() {
          this.inProgress = false;
          this.progressColor = undefined;
        },

        setHealth: function(success) {
          if (!success.enabled) {
            this.error = success.disabledReason;
          }
        },

      };
    }

    var inputs = [];
    var outputs = [];
    var outputMap = {};
    var i;
    for (i = 0; i < metadata.inputs.length; ++i) {
      inputs.push(plugWrapper(metadata.inputs[i], i, 'inputs'));
    }
    for (i = 0; i < metadata.outputs.length; ++i) {
      var plug = plugWrapper(metadata.outputs[i], i, 'outputs');
      outputs.push(plug);
      outputMap[plug.id] = plug;
    }
    function getCommentLines() {
      var comment;
      if (metadata.operationID === 'Add comment') {
        comment = instance.parameters.comment;
      } else if (metadata.operationID === 'Anchor') {
        comment = instance.parameters.description;
      }
      return comment ? comment.split('\n') : [];
    }

    return {
      metadata: metadata,
      instance: instance,
      inputs: inputs,
      outputs: outputs,
      outputMap: outputMap,
      width: width,
      height: height,
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
  };
});
