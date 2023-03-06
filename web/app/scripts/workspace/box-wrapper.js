'use strict';
import md from "markdown-it";
import '../app';
import './plug-wrapper';

// Creates a box object for a workspace.
// This object wraps the actual box data representation and
// provides methods to interact with the GUI, e.g. mouse events
// and values to bind with Angular to SVG elements.
// The wrapped box data representation has two parts:
// - metadata describes the type of operation of the box.
// - instance describes the operation parameter values,
//   coordinates on the workspace and everything related to this
//   box instance that have to be saved.

angular.module('biggraph').factory('BoxWrapper', function(PlugWrapper) {
  function getComment(metadata, instance) {
    let comment;
    if (metadata.operationId === 'Comment') {
      comment = instance.parameters.comment;
    } else if (metadata.operationId === 'Anchor') {
      comment = instance.parameters.description;
    }
    return comment ? md.render(comment) : undefined;
  }

  function BoxWrapper(workspace, metadata, instance, opts) {
    this.workspace = workspace;
    this.metadata = metadata;
    this.instance = instance;
    this.isDirty = opts.isDirty;
    this.summary = metadata.operationId;
    this.inputs = [];
    this.outputs = [];
    this.outputMap = {};
    this.width = 100;
    this.height = 100;
    this.comment = getComment(metadata, instance);

    let i;
    for (i = 0; i < metadata.inputs.length; ++i) {
      this.inputs.push(new PlugWrapper(workspace, metadata.inputs[i], i, 'inputs', this));
    }
    for (i = 0; i < metadata.outputs.length; ++i) {
      const plug = new PlugWrapper(workspace, metadata.outputs[i], i, 'outputs', this);
      this.outputs.push(plug);
      this.outputMap[plug.id] = plug;
    }
  }

  BoxWrapper.prototype = {
    cx: function() { return this.instance.x + this.width / 2; },
    cy: function() { return this.instance.y + this.height / 2; },
    mainPosTransform: function() {
      return 'translate(' + this.instance.x + ', ' + this.instance.y + ')';
    },
    onMouseMove: function(event) {
      let newX = event.logicalX + this.xOffset;
      let newY = event.logicalY + this.yOffset;
      if (event.shiftKey) {
        const G = 50; // Snap grid size.
        newX = G * Math.round(newX / G);
        newY = G * Math.round(newY / G);
      }
      if (newX !== this.instance.x || newY !== this.instance.y) {
        this.workspace._requestInvalidated = true;
        this.isDirty = true;
        this.instance.x = newX;
        this.instance.y = newY;
      }
    },
    onMouseDown: function(event) {
      this.xOffset = this.instance.x - event.logicalX;
      this.yOffset = this.instance.y - event.logicalY;
    },
    inProgress: function() {
      for (let p of this.outputs) {
        if (p.progress === 'in-progress') {
          return true;
        }
      }
    },
  };

  return BoxWrapper;
});
