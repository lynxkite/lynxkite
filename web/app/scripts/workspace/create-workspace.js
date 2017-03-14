'use strict';

// Creates a workspace object. A workspace is a set of boxes and arrows.
// This object wraps the actual workspace data representation and
// provides methods to interact with the GUI, e.g. mouse events
// and values to bind with Angular to SVG elements.
// Every time the underlying workspace data was significantly changed, the
// build() method needs to be invoked to rebuild the frontend-facing data.

angular.module('biggraph').factory('createWorkspace', function(createBox) {
  return function(rawWorkspace, boxCatalogMap) {
    var workspace = {
      // Original list of boxes. This is what we save and load.
      rawBoxes: rawWorkspace.boxes,
      // The below data structures are generated from rawBoxes
      // by this.build(). These are the ones that interact with
      // the GUI.
      boxes: [],
      arrows: [],
      boxMap: {},

      rawWorkspace: function() {
        return {
          boxes: this.rawBoxes
        };
      },

      // private
      buildBoxes: function() {
        this.boxes = [];
        this.boxMap = {};
        var box;
        for (var i = 0; i < this.rawBoxes.length; ++i) {
          var rawBox = this.rawBoxes[i];
          var operationId = rawBox.operationID;
          var boxId = rawBox.id;
          box = createBox(boxCatalogMap[operationId], rawBox);
          this.boxes[i] = box;
          this.boxMap[boxId] = box;
        }
      },

      // private static
      lookupArrowEndpoint: function(list, id) {
        for (var i = 0; i < list.length; ++i) {
          if (list[i].data.id === id) {
            return list[i];
          }
        }
        return undefined;
      },

      // private static
      createArrow: function(srcPlug, dstPlug) {
        return {
          x1: srcPlug.x,
          y1: srcPlug.y,
          x2: dstPlug.x,
          y2: dstPlug.y,
        };
      },

      // private
      buildArrows: function() {
        this.arrows = [];
        for (var i = 0; i < this.boxes.length; ++i) {
          var dst = this.boxes[i];
          var inputs = dst.instance.inputs;
          for (var inputName in inputs) {
            if (inputs.hasOwnProperty(inputName)) {
              var input = inputs[inputName];
              var src = this.boxMap[input.boxID];
              var srcPlug = this.lookupArrowEndpoint(
                src.outputs, input.id);
              var dstPlug = this.lookupArrowEndpoint(
                dst.inputs, inputName);
              this.arrows.push(this.createArrow(
                srcPlug, dstPlug));
            }
          }
        }
      },

      build: function() {
        this.buildBoxes();
        this.buildArrows();
      },

      addBox: function(operationId, x, y) {
        var cnt = this.boxes.length;
        var boxId = operationId + cnt;

        this.rawBoxes.push(
            {
              id: boxId,
              operationID: operationId,
              x: x,
              y: y,
              inputs: {},
              parameters: {}
            });
        this.build();
      },

      addArrow: function(plug1, plug2) {
        if (plug1.direction === plug2.direction) {
          return false;
        }
        var plugs = {};
        plugs[plug1.direction] = plug1;
        plugs[plug2.direction] = plug2;
        var src = plugs.outputs;
        var dst = plugs.inputs;

        dst.instance.inputs[dst.data.id] = {
          boxID: src.boxId,
          id: src.data.id
        };

        this.build();
        return true;
      },

      updateProgress: function(progressInfo) {
        for (var i = 0; i < progressInfo.length; i++) {
          var output =  progressInfo[i];
          var plugDescription = output.a;
          var box = this.boxMap[plugDescription.boxID];
          var plug = box.outputMap[plugDescription.id];
          plug.updateProgress(output.b);
        }
      },

      clearProgress : function() {
        for (var i = 0; i < this.boxes.length; i++) {
          var box = this.boxes[i];
          for (var j = 0; j < box.outputs.length; j++) {
            box.outputs[j].updateProgress(undefined);
          }
        }
      },

    };

    workspace.build();
    return workspace;
  };
});
