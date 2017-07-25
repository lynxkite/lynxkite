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

angular.module('biggraph').factory('WorkspaceWrapper', function(BoxWrapper, util, $interval) {
  function WorkspaceWrapper(name, boxCatalog) {
    this._progressUpdater = undefined;
    this.boxCatalog = boxCatalog;  // Updated for the sake of the operation palette.
    this._boxCatalogMap = undefined;
    this.name = name;
    this.top = name;
    this.customBoxStack = [];
    this.state = undefined;
    // The below data structures are generated from rawBoxes
    // by this.build(). These are the ones that interact with
    // the GUI.
    this.boxes = [];
    this.arrows = [];
    this.boxMap = {};
    // Immutable backup of the backend state from last
    // request:
    this.backendRequest = undefined;
    this.backendState = undefined;
  }

  WorkspaceWrapper.prototype = {
    ref: function() {  // Returns a WorkspaceReference object.
      return { top: this.top, customBoxStack: this.customBoxStack };
    },

    _updateBoxCatalog: function() {
      var that = this;
      var request = util.nocache('/ajax/boxCatalog');
      angular.merge(that.boxCatalog, request);
      return request.then(function(bc) {
        angular.merge(that.boxCatalog, request);
        that._boxCatalogMap = {};
        for (var i = 0; i < bc.boxes.length; ++i) {
          var boxMeta = bc.boxes[i];
          that._boxCatalogMap[boxMeta.operationId] = boxMeta;
        }
      });
    },

    _buildBoxes: function() {
      this.boxes = [];
      this.boxMap = {};
      for (var i = 0; i < this.state.boxes.length; ++i) {
        this._addBoxWrapper(this.state.boxes[i]);
      }
    },

    _addBoxWrapper: function(rawBox) {
      var box = new BoxWrapper(this, this._boxCatalogMap[rawBox.operationId], rawBox);
      this.boxes.push(box);
      this.boxMap[rawBox.id] = box;
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
      if (srcPlug && dstPlug) {
        return {
          src: srcPlug,
          dst: dstPlug,
          x1: function() { return srcPlug.cx(); },
          y1: function() { return srcPlug.cy(); },
          x2: function() { return dstPlug.cx(); },
          y2: function() { return dstPlug.cy(); },
        };
      } else {
        // TODO: Add some kind of visual indicator to direct the user's attention to the erroneous plugs.
        return undefined;
      }
    },

    _buildArrows: function() {
      this.arrows = [];
      for (var i = 0; i < this.boxes.length; ++i) {
        var dst = this.boxes[i];
        var inputs = dst.instance.inputs;
        for (var inputName in inputs) {
          if (inputs.hasOwnProperty(inputName)) {
            var input = inputs[inputName];
            var src = this.boxMap[input.boxId];
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
      this.name = response.name;
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
      this.stopProgressUpdate();
      var that = this;
      this._progressUpdater = $interval(function() {
        that._getAndUpdateProgress();
      }, 2000);
    },

    stopProgressUpdate: function() {
      if (this._progressUpdater) {
        $interval.cancel(this._progressUpdater);
        this._progressUpdater = undefined;
      }
    },

    _getAndUpdateProgress: function() {
      if (this.knownStateIds) {
        var that = this;
        var lastProgressRequest = that.lastProgressRequest = util.nocache('/ajax/getProgress', {
          stateIds: that.knownStateIds,
        }).then(
          function onSuccess(response) {
            if (lastProgressRequest === that.lastProgressRequest) {
              that.updateProgress(response.progress);
            }
          },
          function onError(error) {
            /* eslint-disable no-console */
            console.error('Couldn\'t get progress information.', error);
          });
      }
    },
    _lastLoadRequest: undefined,
    _requestInvalidated: false,
    loadWorkspace: function(workspaceStateRequest) {
      var that = this;
      if (!this._boxCatalogMap) { // Need to load catalog first.
        this._updateBoxCatalog().then(function() { that.loadWorkspace(workspaceStateRequest); });
        return;
      }
      var request = workspaceStateRequest || util.nocache('/ajax/getWorkspace', this.ref());
      this._lastLoadRequest = request;
      this._requestInvalidated = false;
      request
        .then(
          function onSuccess(response) {
            if (request === that._lastLoadRequest && !that._requestInvalidated) {
              that._init(response);
            }
          },
          function onError(error) {
            that.error = util.responseToErrorMessage(error);
          })
        .then(function() {
          that._startProgressUpdate();
        });
    },

    saveWorkspace: function() {
      var that = this;
      that.loadWorkspace(
        util.post('/ajax/setWorkspace', { reference: that.ref(), workspace: that.state }));
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

    _addBox: function(operationId, x, y, opts) {
      opts = opts || {};
      // opts.boxId should be used for test-purposes only
      var boxId = opts.boxId || this.getUniqueId(operationId);
      // Create a box backend data structure, an unwrapped box:
      var box = {
        id: boxId,
        operationId: operationId,
        x: x,
        y: y,
        inputs: {},
        parameters: {},
        parametricParameters: {}
      };
      this.state.boxes.push(box);
      this._addBoxWrapper(box);
      return box;
    },

    addBox: function(operationId, event, opts) {
      opts = opts || {};
      var box = this._addBox(
          operationId,
          event.logicalX,
          event.logicalY,
          opts);
      if (!opts.willSaveLater) {
        this.saveWorkspace();
      }
      return box;
    },

    _deleteBox: function(boxId) {
      this.state.boxes = this.state.boxes.filter(function(box) {
        return box.id !== boxId;
      });
      this.state.boxes.map(function(box) {
        var inputs = box.inputs;
        for (var inputName in inputs) {
          if (inputs[inputName].boxId === boxId) {
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

    addArrow: function(plug1, plug2, opts) {
      opts = opts || {};
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
        boxId: src.boxId,
        id: src.id
      };
      // Rebuild API objects based on raw workflow:
      this._buildArrows();
      if (!opts.willSaveLater) {
        this.saveWorkspace();
      }
      return true;
    },

    _assignStateInfoToPlugs: function(stateInfo) {
      this.knownStateIds = [];
      this.stateId2Plug = {};
      for (var i = 0; i < stateInfo.length; i++) {
        var item = stateInfo[i];
        var boxOutput = item.boxOutput;
        var stateId = item.stateId;
        this.knownStateIds.push(stateId);
        var box = this.boxMap[boxOutput.boxId];
        var plug = box.outputMap[boxOutput.id];
        plug.stateId = stateId;
        plug.setHealth(item.success);
        plug.kind = item.kind;
        this.stateId2Plug[stateId] = plug;
      }
    },

    _assignSummaryInfoToBoxes: function(summaries) {
      for (var i = 0; i < this.boxes.length; i++) {
        var box = this.boxes[i];
        box.summary = summaries[box.instance.id];
        if (!box.summary) {
          box.summary = box.metadata.operationId;
        }
      }
    },

    updateProgress: function(progressMap) {
      for (var stateId in progressMap) {
        if (progressMap.hasOwnProperty(stateId)) {
          var progress = progressMap[stateId];
          // failed states has 'undefined' as progress
          if (progress) {
            var plug = this.stateId2Plug[stateId];
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

    getWidthFromInstance: function(box) {
      var wrapper = new BoxWrapper(this, this._boxCatalogMap[box.operationId], box);
      return wrapper.width;
    },

    pasteFromData: function(boxes, currentPosition) {
      var mapping = {};
      var newBoxes = [];
      for (var i = 0; i < boxes.length; ++i) {
        var box = boxes[i];
        var diffX = this.getWidthFromInstance(box);
        var createdBox = this._addBox(
          box.operationId,
          currentPosition.logicalX + box.x + 1.1 * diffX,
          currentPosition.logicalY + box.y + 10);
        createdBox.parameters = box.parameters;
        createdBox.parametricParameters = box.parametricParameters;
        mapping[box.id] = createdBox;
        newBoxes.push(createdBox);
      }
      for (i = 0; i < boxes.length; ++i) {
        var oldBox = boxes[i];
        var newBox = mapping[oldBox.id];
        for (var key in oldBox.inputs) {
          if (!oldBox.inputs.hasOwnProperty(key)) {
            break;
          }
          var oldInputId = oldBox.inputs[key].boxId;
          if (mapping.hasOwnProperty(oldInputId)) {
            var newInput = mapping[oldInputId];
            newBox.inputs[key] = { boxId: newInput.id, id: oldBox.inputs[key].id };
          }
        }
      }
      this.saveWorkspace();
      return newBoxes;
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
      that.loadWorkspace(util.post('/ajax/undoWorkspace', that.ref()));
    },

    redo: function() {
      if (!this.canRedo()) { return; }
      var that = this;
      that.loadWorkspace(util.post('/ajax/redoWorkspace', that.ref()));
    },

    saveAsCustomBox: function(ids, name, description) {
      var i, j, box, input;
      var workspaceParameters =
        JSON.parse(this.boxMap['anchor'].instance.parameters.parameters || '[]');
      var boxes = [{
        id: 'anchor',
        operationId: 'Anchor',
        x: 0,
        y: 0,
        inputs: {},
        parameters: {
          description: description,
          // The new workspace simply inherits all the parameters from the current workspace.
          parameters: JSON.stringify(workspaceParameters),
        },
        parametricParameters: {},
      }];
      var inputNameCounts = {};
      var outputNameCounts = {};
      var SEPARATOR = ', ';
      var PADDING_X = 200;
      var PADDING_Y = 150;
      var inputBoxY = PADDING_Y;
      var outputBoxY = PADDING_Y;
      var internallyUsedOutputs = {};
      var externallyUsedOutputCounts = {};
      // This custom box will replace the selected boxes.
      var customBox = {
        id: this.getUniqueId(name),
        operationId: name,
        x: 0,
        y: 0,
        inputs: {},
        parameters: {},
        parametricParameters: {},
      };
      // Pass all workspace parameters through.
      for (i = 0; i < workspaceParameters; ++i) {
        var param = workspaceParameters[i].id;
        customBox.parametricParameters[param] = '$' + param;
      }
      var minX = Infinity;
      var minY = Infinity;
      var maxX = -Infinity;
      for (i = 0; i < ids.length; ++i) {
        box = this.boxMap[ids[i]];
        // Place the custom box in the average position of the selected boxes.
        customBox.x += box.instance.x / ids.length;
        customBox.y += box.instance.y / ids.length;
        minX = Math.min(minX, box.instance.x);
        minY = Math.min(minY, box.instance.y);
        maxX = Math.max(maxX, box.instance.x);
      }
      // Record used outputs.
      for (i = 0; i < this.boxes.length; ++i) {
        box = this.boxes[i];
        var inputs = Object.keys(box.instance.inputs);
        for (j = 0; j < inputs.length; ++j) {
          input = box.instance.inputs[inputs[j]];
          if (input.boxId) {
            console.assert(!input.boxId.includes(SEPARATOR) && !input.id.includes(SEPARATOR));
            var key = input.boxId + SEPARATOR + input.id;
            externallyUsedOutputCounts[key] = (externallyUsedOutputCounts[key] || 0) + 1;
          }
        }
      }
      for (i = 0; i < ids.length; ++i) {
        box = this.boxMap[ids[i]];
        // "input-" and "output-" IDs will be used for the input and output boxes.
        console.assert(box.instance.id.indexOf('input-') !== 0);
        console.assert(box.instance.id.indexOf('output-') !== 0);
        if (ids[i] === 'anchor') { continue; }  // Ignore anchor.
        // Copy this box to the new workspace.
        var instance = angular.copy(box.instance);
        boxes.push(instance);
        instance.x += PADDING_X - minX;
        instance.y += PADDING_Y / 2 - minY;
        for (j = 0; j < box.inputs.length; ++j) {
          var inputName = box.metadata.inputs[j];
          input = box.instance.inputs[inputName];
          // Record internally used output.
          if (input.boxId) {
            console.assert(!input.boxId.includes(SEPARATOR) && !input.id.includes(SEPARATOR));
            externallyUsedOutputCounts[input.boxId + SEPARATOR + input.id] -= 1;
            internallyUsedOutputs[input.boxId + SEPARATOR + input.id] = true;
          }
          // Create input box if necessary.
          if (!ids.includes(input.boxId)) {
            var inputBoxName = inputName;
            var inputNameCount = inputNameCounts[inputName] || 0;
            if (inputNameCount > 0) {
              inputBoxName += ' ' + (inputNameCount + 1);
            }
            inputNameCounts[inputName] = inputNameCount + 1;
            boxes.push({
              id: 'input-' + inputBoxName,
              operationId: 'Input',
              x: 0,
              y: inputBoxY,
              inputs: {},
              parameters: { name: inputBoxName },
              parametricParameters: {},
            });
            inputBoxY += PADDING_Y;
            instance.inputs[inputName] = { boxId: 'input-' + inputBoxName, id: 'input' };
            if (input.boxId) { // Connected to a non-selected box.
              customBox.inputs[inputBoxName] = input;
            }
          }
        }
      }
      // Add output boxes as necessary.
      for (i = 0; i < ids.length; ++i) {
        box = this.boxMap[ids[i]];
        for (j = 0; j < box.metadata.outputs.length; ++j) {
          var outputName = box.metadata.outputs[j];
          if (!internallyUsedOutputs[box.instance.id + SEPARATOR + outputName] ||
              externallyUsedOutputCounts[box.instance.id + SEPARATOR + outputName] > 0) {
            var outputBoxName = outputName;
            var outputNameCount = outputNameCounts[outputName] || 0;
            if (outputNameCount > 0) {
              outputBoxName += ' ' + (outputNameCount + 1);
            }
            outputNameCounts[outputName] = outputNameCount + 1;
            boxes.push({
              id: 'output-' + outputBoxName,
              operationId: 'Output',
              x: maxX - minX + PADDING_X * 2,
              y: outputBoxY,
              inputs: { output: { boxId: box.instance.id, id: outputName } },
              parameters: { name: outputBoxName },
              parametricParameters: {},
            });
            outputBoxY += PADDING_Y;
            // Update non-selected output connections.
            for (var k = 0; k < this.arrows.length; ++k) {
              var arrow = this.arrows[k];
              if (arrow.src.boxId === box.instance.id && arrow.src.id === outputName) {
                arrow.dst.box.instance.inputs[arrow.dst.id] =
                  { boxId: customBox.id, id: outputBoxName };
              }
            }
          }
        }
      }
      this.state.boxes = this.state.boxes.filter(function(box) {
        return box.id === 'anchor' || !ids.includes(box.id);
      });
      this.state.boxes.push(customBox);
      var that = this;
      return {
        customBox: customBox,
        promise: util.post('/ajax/createWorkspace', {
          name: name,
        }).then(function success() {
          return util.post('/ajax/setWorkspace', {
            reference: { top: name, customBoxStack: [] },
            workspace: { boxes: boxes },
          });
        }).then(function success() {
          return that._updateBoxCatalog();
        }).then(function success() {
          that.saveWorkspace();
        }, function error() {
          that.loadWorkspace();
        }),
      };
    },

    startSavingAs: function() {
      this.showSaveAs = true;
      this.saveAsName = this.name;
    },

    maybeSaveAs: function() {
      // We only need to do an actual action if the user has changed the name.
      if (this.saveAsName !== this.name) {
        this.saveAs(this.saveAsName);
      }
      this.showSaveAs = false;
    },

    saveAs: function(newName) {
      util.post('/ajax/forkEntry',
        {
          from: this.name,
          to: newName,
        }).then(function() {
          window.location = '#/workspace/' + newName;
        });
    },

  };

  return WorkspaceWrapper;
});
