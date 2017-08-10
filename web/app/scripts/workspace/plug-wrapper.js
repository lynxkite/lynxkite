'use strict';

// Creates a plug object for a workspace. A plug is an input or an output of a box
// and it is represented by a circle on the GUI.
// This object wraps the actual plug data representation, has a pointer to
// to the box, provides methods to interact with the GUI (mouse events)
// and values to bind with Angular to SVG elements.

angular.module('biggraph').factory('PlugWrapper', function() {
  function PlugWrapper(workspace, id, index, direction, box) {
    this.workspace = workspace;
    this.rx = direction === 'outputs' ? box.width : 0;
    var count = box.metadata[direction].length;
    this.ry = box.height - 20 * (count - index);
    this.box = box;
    this.boxId = box.instance.id;
    this.boxInstance = box.instance;
    this.id = id;
    this.direction = direction;
    this.radius = 8;
    this.posTransform = 'translate(' + this.rx + ', ' + this.ry + ')';
    this.progress = 'unknown';
    this.error = undefined;
  }

  PlugWrapper.prototype = {
    cx: function() { return this.rx + this.box.instance.x; },
    cy: function() { return this.ry + this.box.instance.y; },

    updateProgress: function(progress) {
      if (progress.inProgress) {
        this.progress = 'in-progress';
      } else if (progress.failed) {
        this.progress = 'error';
      } else if (progress.notYetStarted) {
        this.progress = 'not-complete';
      } else {
        this.progress = 'complete';
      }
    },

    setHealth: function(success) {
      if (success.enabled) {
        this.error = undefined;
      } else {
        this.error = success.disabledReason;
      }
    },

    getAttachedPlugs: function() {
      var conn;
      if (this.direction === 'inputs') {
        conn = this.boxInstance.inputs[this.id];
        return conn ? [this.workspace.getOutputPlug(conn.boxId, conn.id)] : [];
      } else {
        var dsts = [];
        for (var i = 0; i < this.workspace.boxes.length; ++i) {
          var box = this.workspace.boxes[i];
          for (var j = 0; j < box.inputs.length; ++j) {
            var input = box.inputs[j];
            conn = box.instance.inputs[input.id];
            if (conn && conn.boxId === this.boxId && conn.id === this.id) {
              dsts.push(input);
            }
          }
        }
        return dsts;
      }
    },
  };

  return PlugWrapper;
});
