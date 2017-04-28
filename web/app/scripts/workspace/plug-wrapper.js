'use strict';

// Creates a plug object for a workspace. A plug is an input or an output of a box
// and it is represented by a circle on the GUI.
// This object wraps the actual plug data representation, has a pointer to
// to the box, provides methods to interact with the GUI (mouse events)
// and values to bind with Angular to SVG elements.

angular.module('biggraph').factory('plugWrapper', function() {
  return function(id, index, direction, box) {
    var radius = 8;

    var len = box.metadata[direction].length;
    var x;
    if (len <= 1) {
      x = box.width / 2;
    } else {
      x = index * (box.width - radius * 2) / (len - 1) + radius;
    }
    var y = direction === 'outputs' ? box.height + 15 : -15;

    function progressToColor(progressRatio) {
      /* global tinycolor */
      return tinycolor.mix('blue', 'green', progressRatio * 100).toHexString();
    }

    return {
      boxId: box.instance.id,
      boxInstance: box.instance,
      id: id,
      direction: direction,
      radius: radius,
      x: function() { return x + box.instance.x; },
      y: function() { return y + box.instance.y; },
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
  };
});
