'use strict';

// Creates a workspace wrapper object. A workspace is a set of boxes and arrows.
// This object wraps the actual workspace data representation and
// provides methods to interact with the GUI, e.g. mouse events
// and values to bind with Angular to SVG elements. It also manages
// the life-cycle of the workspace state.
//
// Every time the underlying workspace data was changed, the following flow needs to be executed:
// 1. user change happens, which updates the "raw" state in
//    this.state
// 2. this._build() updates this.boxes, this.arrows and this.boxMap based
//   on this.state. (This updates the GUI.)
// 3. this.saveWorkspace() upload this.state to the server
// 4. this.loadWorkspace() downloads the new state (what was sent
//    in the previous point plus what the server has changed).
// 5. this._build() is invoked again

angular.module('biggraph').factory('workspaceWrapper', function(boxWrapper, util, $interval) {
  return function(name, boxCatalog) {
    var progressUpdater;
    var boxCatalogMap = {};
    for (var i = 0; i < boxCatalog.boxes.length; ++i) {
      var boxMeta = boxCatalog.boxes[i];
      boxCatalogMap[boxMeta.operationID] = boxMeta;
    }

    return {
      name: name,

      state: undefined,
      // The below data structures are generated from rawBoxes
      // by this.build(). These are the ones that interact with
      // the GUI.
      boxes: [],
      arrows: [],
      boxMap: {},
      // Immutable backup of the backend state from last
      // request:
      backendRequest: undefined,
      backendState: undefined,

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
          src: srcPlug,
          dst: dstPlug,
          x1: function() { return srcPlug.cx(); },
          y1: function() { return srcPlug.cy(); },
          x2: function() { return dstPlug.cx(); },
          y2: function() { return dstPlug.cy(); },
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

      _init: function(response) {
        this.backendResponse = response;
        this.backendState = response.workspace;
        // User edits will be applied to a deep copy of
        // the original backend state. This way watchers
        // of backendState will only be notified once the
        // backend is fully aware of the new state.
        this.state = angular.copy(this.backendState);
        this._build();
        this._assignStateInfoToPlugs(response.outputs);
        this._assignSummaryInfoToBoxes(response.summaries);
      },

      _startProgressUpdate: function() {
        this._stopProgressUpdate();
        var that = this;
        progressUpdater = $interval(function() {
          that._getAndUpdateProgress();
        }, 2000);
      },

      _stopProgressUpdate: function() {
        if (progressUpdater) {
          $interval.cancel(progressUpdater);
          progressUpdater = undefined;
        }
      },

      _getAndUpdateProgress: function() {
        if (this.knownStateIDs) {
          var that = this;
          var lastProgressRequest = that.lastProgressRequest = util.nocache('/ajax/getProgress', {
            stateIDs: that.knownStateIDs,
          }).then(
            function success(response) {
              if (lastProgressRequest === that.lastProgressRequest) {
                that.updateProgress(response.progress);
              }
            },
            function onerror(error) {
              /* eslint-disable no-console */
              console.error('Couldn\'t get progress information.', error);
            });
        }
      },

      loadWorkspace: function() {
        var that = this;
        util.nocache(
          '/ajax/getWorkspace',
          {
            name: this.name
          })
          .then(function(response) {
            that._init(response);
          })
          .then(function() {
            that._startProgressUpdate();
          });
      },

      saveWorkspace: function() {
        var that = this;
        util.post(
          '/ajax/setWorkspace',
          {
            name: this.name,
            workspace: that.state,
          }).finally(
            // Reload workspace both in error and success cases.
            function() { that.loadWorkspace(); });
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
      _addBox: function(operationId, x, y, boxId) {
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

      // boxID should be used for test-purposes only
      addBox: function(operationId, event, boxID) {
        var box = this._addBox(
            operationId,
            event.logicalX,
            event.logicalY,
            boxID);
        this.saveWorkspace();
        return box;
      },

      _deleteBox: function(boxId) {
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

      deleteBoxes: function(boxIds) {
        for (var i = 0; i < boxIds.length; i += 1) {
          if (boxIds[i] !== 'anchor') {
            this._deleteBox(boxIds[i]);
          }
        }
        this.saveWorkspace();
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
        this.saveWorkspace();
        return true;
      },

      _assignStateInfoToPlugs: function(stateInfo) {
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

      _assignSummaryInfoToBoxes: function(summaries) {
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

      _setBoxParams: function(boxId, plainParamValues, parametricParamValues) {
        this.boxMap[boxId].instance.parameters = plainParamValues;
        this.boxMap[boxId].instance.parametricParameters = parametricParamValues;
      },

      updateBox: function(id, plainParamValues, parametricParamValues) {
        var box = this.boxMap[id].instance;
        if (!angular.equals(plainParamValues, box.parameters) ||
            !angular.equals(parametricParamValues, box.parametricParameters)) {
          this._setBoxParams(id, plainParamValues, parametricParamValues);
          this.saveWorkspace();
        }
      },

      pasteFromClipboard: function(clipboard, currentPosition) {
        var mapping = {};
        for (var i = 0; i < clipboard.length; ++i) {
          var box = clipboard[i].instance;
          var diffX = clipboard[i].width;
          var createdBox = this._addBox(
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
        this.saveWorkspace();
      },

      saveIfBoxesMoved: function() {
        for (var i = 0; i < this.boxes.length; i++) {
          if (this.boxes[i].isMoved) {
            this.saveWorkspace();
            break;
          }
        }
      },

      getBox: function(id) {
        return this.boxMap[id];
      },

      getOutputPlug: function(boxId, plugId) {
        return this.getBox(boxId).outputMap[plugId];
      },

      canUndo: function() { return this.backendResponse && this.backendResponse.canUndo; },
      canRedo: function() { return this.backendResponse && this.backendResponse.canRedo; },

      undo: function() {
        if (!this.canUndo()) { return; }
        var that = this;
        util.post('/ajax/undoWorkspace', { name: this.name })
          .then(function() { that.loadWorkspace(); });
      },

      redo: function() {
        if (!this.canRedo()) { return; }
        var that = this;
        util.post('/ajax/redoWorkspace', { name: this.name })
          .then(function() { that.loadWorkspace(); });
      },

    };
  };
});
