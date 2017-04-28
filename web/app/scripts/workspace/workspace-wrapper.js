'use strict';

// Creates a workspace object. A workspace is a set of boxes and arrows.
// This object wraps the actual workspace data representation and
// provides methods to interact with the GUI, e.g. mouse events
// and values to bind with Angular to SVG elements.
//
// Every time the underlying workspace data was significantly changed, the
// _build() method is invoked to rebuild the frontend-facing data.
// The flow of changes here is always one-way:
// 1. user change or new workspace state loaded
// 2. "raw" state is updated
// 3. _build() updates the frontend-facing objects

angular.module('biggraph').factory('workspaceWrapper', function(boxWrapper) {
  return function(backendResponse, boxCatalogMap) {
    var backendState = backendResponse.workspace;
    // User edits will be applied to a deep copy of
    // the original backend state. This way watchers
    // of backendState will only be notified once the
    // backend is fully aware of the new state.
    var state = angular.copy(backendState);
    var wrapper = {
      state: state,
      // The below data structures are generated from rawBoxes
      // by this.build(). These are the ones that interact with
      // the GUI.
      boxes: [],
      arrows: [],
      boxMap: {},
      // Immutable backup of the backend state from last
      // request:
      backendState: backendState,

      _buildBoxes: function() {
        this.boxes = [];
        this.boxMap = {};
        var box;
        for (var i = 0; i < this.state.boxes.length; ++i) {
          var rawBox = this.state.boxes[i];
          var operationId = rawBox.operationID;
          var boxId = rawBox.id;
          box = boxWrapper(boxCatalogMap[operationId], rawBox);
          this.boxes[i] = box;
          this.boxMap[boxId] = box;
        }
      },

      _lookupArrowEndpoint: function(list, id) {
        for (var i = 0; i < list.length; ++i) {
          if (list[i].id === id) {
            return list[i];
          }
        }
        return undefined;
      },

      _createArrow: function(srcPlug, dstPlug) {
        return {
          x1: srcPlug.x,
          y1: srcPlug.y,
          x2: dstPlug.x,
          y2: dstPlug.y,
        };
      },

      _buildArrows: function() {
        this.arrows = [];
        for (var i = 0; i < this.boxes.length; ++i) {
          var dst = this.boxes[i];
          var inputs = dst.instance.inputs;
          for (var inputName in inputs) {
            if (inputs.hasOwnProperty(inputName)) {
              var input = inputs[inputName];
              var src = this.boxMap[input.boxID];
              if (src) {
                var srcPlug = this._lookupArrowEndpoint(
                  src.outputs, input.id);
                var dstPlug = this._lookupArrowEndpoint(
                  dst.inputs, inputName);
                this.arrows.push(this._createArrow(
                  srcPlug, dstPlug));
              }
            }
          }
        }
      },

      // If state was updated, this needs to run so that the frontend-facing objects
      // are also updated.
      _build: function() {
        this._buildBoxes();
        this._buildArrows();
      },

      getUniqueId: function(operationId) {
        var usedIds = this.state.boxes.map(function(box) {
          return box.id;
        });
        var cnt = 1;
        while (usedIds.includes(operationId.replace(/ /g, '-') + '_' + cnt)) {
          cnt += 1;
        }
        return operationId.replace(/ /g, '-') + '_' + cnt;
      },

      // boxID should be used for test-purposes only
      addBox: function(operationId, x, y, boxId) {
        boxId = boxId || this.getUniqueId(operationId);
        // Create a box backend data structure, an unwrapped box:
        var box = {
          id: boxId,
          operationID: operationId,
          x: x,
          y: y,
          inputs: {},
          parameters: {},
          parametricParameters: {}
        };
        this.state.boxes.push(box);
        // Rebuild box wrappers:
        this._build();
        return box;
      },

      deleteBox: function(boxId) {
        this.state.boxes = this.state.boxes.filter(function(box) {
          return box.id !== boxId;
        });
        this.state.boxes.map(function(box) {
          var inputs = box.inputs;
          for (var inputName in inputs) {
            if (inputs[inputName].boxID === boxId) {
              delete box.inputs[inputName];
            }
          }
        });
        this._build();
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

        // Mutate raw workflow:
        dst.boxInstance.inputs[dst.id] = {
          boxID: src.boxId,
          id: src.id
        };
        // Rebuild API objects based on raw workflow:
        this._build();
        return true;
      },

      assignStateInfoToPlugs: function(stateInfo) {
        this.knownStateIDs = [];
        this.stateID2Plug = {};
        for (var i = 0; i < stateInfo.length; i++) {
          var item = stateInfo[i];
          var boxOutput = item.boxOutput;
          var stateID = item.stateID;
          this.knownStateIDs.push(stateID);
          var box = this.boxMap[boxOutput.boxID];
          var plug = box.outputMap[boxOutput.id];
          plug.stateID = stateID;
          plug.setHealth(item.success);
          plug.kind = item.kind;
          this.stateID2Plug[stateID] = plug;
        }
      },

      assignSummaryInfoToBoxes: function(summaries) {
        for (var i = 0; i < this.boxes.length; i++) {
          var box = this.boxes[i];
          box.summary = summaries[box.id];
          if (!box.summary) {
            box.summary = box.metadata.operationID;
          }
        }
      },

      updateProgress: function(progressMap) {
        for (var stateID in progressMap) {
          if (progressMap.hasOwnProperty(stateID)) {
            var progress = progressMap[stateID];
            // failed states has 'undefined' as progress
            if (progress) {
              var plug = this.stateID2Plug[stateID];
              if (plug) {
                plug.updateProgress(progress);
              }
            }
          }
        }
      },

      clearProgress: function() {
        for (var i = 0; i < this.boxes.length; i++) {
          var box = this.boxes[i];
          for (var j = 0; j < box.outputs.length; j++) {
            box.outputs[j].clearProgress();
          }
        }
      },

      setBoxParams: function(boxId, plainParamValues, parametricParamValues) {
        this.boxMap[boxId].instance.parameters = plainParamValues;
        this.boxMap[boxId].instance.parametricParameters = parametricParamValues;
      },

      pasteFromClipboard: function(clipboard, currentPosition) {
        var mapping = {};
        for (var i = 0; i < clipboard.length; ++i) {
          var box = clipboard[i].instance;
          var diffX = clipboard[i].width;
          var createdBox = this.addBox(
            box.operationID,
            currentPosition.logicalX + box.x + 1.1 * diffX,
            currentPosition.logicalY + box.y + 10);
          createdBox.parameters = box.parameters;
          createdBox.parametricParameters = box.parametricParameters;
          mapping[box.id] = createdBox;
        }
        for (i = 0; i < clipboard.length; ++i) {
          var oldBox = clipboard[i].instance;
          var newBox = mapping[oldBox.id];
          for (var key in oldBox.inputs) {
            if (!oldBox.inputs.hasOwnProperty(key)) {
              break;
            }
            var oldInputId = oldBox.inputs[key].boxID;
            if (mapping.hasOwnProperty(oldInputId)) {
              var newInput = mapping[oldInputId];
              newBox.inputs[key] = { boxID: newInput.id, id: key };
            }
          }
        }
      }

    };

    wrapper._build();
    wrapper.assignStateInfoToPlugs(backendResponse.outputs);
    wrapper.assignSummaryInfoToBoxes(backendResponse.summaries);
    return wrapper;
  };
});
