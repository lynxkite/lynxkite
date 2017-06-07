'use strict';

// Creates a plug object for a workspace. A plug is an input or an output of a box
// and it is represented by a circle on the GUI.
// This object wraps the actual plug data representation, has a pointer to
// to the box, provides methods to interact with the GUI (mouse events)
// and values to bind with Angular to SVG elements.

angular.module('biggraph').factory('PlugWrapper', function() {
  function progressToColor(progressRatio) {
    /* global tinycolor */
    return tinycolor.mix('blue', 'green', progressRatio * 100).toHexString();
  }

  function PlugWrapper(id, index, direction, box) {
    var radius = 8;
    var len = box.metadata[direction].length;
    if (len <= 1) {
      this.ry = box.height - 20;
    } else {
      this.ry = index * (box.height - radius * 2) / (len - 1) + radius;
    }
    this.rx = direction === 'outputs' ? box.width : 0;
    this.box = box;
    this.boxId = box.instance.id;
    this.boxInstance = box.instance;
    this.id = id;
    this.direction = direction;
    this.radius = radius;
    this.posTransform = 'translate(' + this.rx + ', ' + this.ry + ')';
    this.inProgress = false;
    this.progressColor = undefined;
    this.error = undefined;
  }

  PlugWrapper.prototype = {
    cx: function() { return this.rx + this.box.instance.x; },
    cy: function() { return this.ry + this.box.instance.y; },

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
      if (success.enabled) {
        this.error = undefined;
      } else {
        this.error = success.disabledReason;
      }
    },
  };

  return PlugWrapper;
});
