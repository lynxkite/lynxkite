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

angular.module('biggraph')
  .factory('WorkspaceWrapper', function(BoxWrapper, PlugWrapper, util, longPoll) {
    function WorkspaceWrapper(name, boxCatalog) {
      this.boxCatalog = boxCatalog; // Updated for the sake of the operation palette.
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
      this.saveCustomBoxAsName = undefined;
    }

    WorkspaceWrapper.prototype = {
      customBoxPath: function() {
        return this.customBoxStack.map(function(x) { return x.id; });
      },

      ref: function() { // Returns a WorkspaceReference object.
        return { top: this.top, customBoxStack: this.customBoxPath() };
      },

      _updateBoxCatalog: function() {
        const that = this;
        const request = util.nocache('/ajax/boxCatalog', {ref: this.ref()});
        angular.merge(that.boxCatalog, request);
        return request.then(function(bc) {
          angular.merge(that.boxCatalog, request);
          that._boxCatalogMap = {};
          for (let i = 0; i < bc.boxes.length; ++i) {
            const boxMeta = bc.boxes[i];
            that._boxCatalogMap[boxMeta.operationId] = boxMeta;
          }
        });
      },

      _buildBoxes: function() {
        this.boxes = [];
        this.boxMap = {};
        for (let i = 0; i < this.state.boxes.length; ++i) {
          this._addBoxWrapper(this.state.boxes[i], { isDirty: false });
        }
      },

      _addBoxWrapper: function(rawBox, opts) {
        const meta = this._boxCatalogMap[rawBox.operationId] || {
        // Defaults for unknown operations.
          categoryId: '',
          icon: 'images/icons/black_question_mark_ornament.png',
          color: 'natural',
          operationId: rawBox.operationId,
          inputs: [],
          outputs: [],
          description: '',
        };
        const box = new BoxWrapper(this, meta, rawBox, opts);
        this.boxes.push(box);
        this.boxMap[rawBox.id] = box;
      },

      _lookupArrowEndpoint: function(box, direction, id) {
        const list = box[direction];
        let i;
        for (i = 0; i < list.length; ++i) {
          if (list[i].id === id) {
            return list[i];
          }
        }
        const plug = new PlugWrapper(this, id, i, direction, box);
        plug.progress = 'missing';
        list.push(plug);
        return plug;
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
        for (let i = 0; i < this.boxes.length; ++i) {
          const dst = this.boxes[i];
          const inputs = dst.instance.inputs;
          for (const [inputName, input] of Object.entries(inputs)) {
            const src = this.boxMap[input.boxId];
            if (src) {
              const srcPlug = this._lookupArrowEndpoint(src, 'outputs', input.id);
              const dstPlug = this._lookupArrowEndpoint(dst, 'inputs', inputName);
              this.arrows.push(this._createArrow(srcPlug, dstPlug));
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
        this.updateProgress(response.progress);
      },

      _lastLoadRequest: undefined,
      _requestInvalidated: false,
      loadWorkspace: function(workspaceStateRequest) {
        const that = this;
        const request = workspaceStateRequest || util.nocache('/ajax/getWorkspace', this.ref());
        this._lastLoadRequest = request;
        this._requestInvalidated = false;
        const promise = request
          .then(function ensureBoxCatalog(response) {
            if (!that._boxCatalogMap) { // Need to load catalog before processing the response.
              return that._updateBoxCatalog().then(function() { return response; });
            } else {
              return response;
            }
          })
          .then(
            function onSuccess(response) {
              that.loaded = true;
              if (request === that._lastLoadRequest && !that._requestInvalidated) {
                that._init(response);
              }
            },
            function onError(error) {
              /* eslint-disable no-console */
              console.error('Failed to load workspace.', error);
              if (that.backendResponse) {
                // We are already displaying a workspace. Revert local changes.
                // A popup will be displayed for the failed edit.
                that._init(that.backendResponse);
              } else {
                // Couldn't load workspace. Display an error in its place.
                that.error = util.responseToErrorMessage(error);
              }
            });
        this.loading = promise;
        return promise;
      },

      saveWorkspace: function() {
        return this.loadWorkspace(
          util.post('/ajax/setAndGetWorkspace', { reference: this.ref(), workspace: this.state }));
      },

      getUniqueId: function(operationId) {
        const usedIds = this.state.boxes.map(function(box) {
          return box.id;
        });
        let cnt = 1;
        while (usedIds.includes(operationId.replace(/ /g, '-') + '_' + cnt)) {
          cnt += 1;
        }
        return operationId.replace(/ /g, '-') + '_' + cnt;
      },

      _addBox: function(operationId, x, y, opts) {
        opts = opts || {};
        // opts.boxId should be used for test-purposes only
        const boxId = opts.boxId || this.getUniqueId(operationId);
        // Create a box backend data structure, an unwrapped box:
        const box = {
          id: boxId,
          operationId: operationId,
          x: x,
          y: y,
          inputs: {},
          parameters: {},
          parametricParameters: {}
        };
        this.state.boxes.push(box);
        this._addBoxWrapper(box, { isDirty: true });
        return box;
      },

      addBox: function(operationId, event, opts) {
        opts = opts || {};
        const box = this._addBox(
          operationId,
          event.logicalX,
          event.logicalY,
          opts);
        if (!opts.willSaveLater) {
          this.saveWorkspace();
        }
        const meta = this._boxCatalogMap[operationId];
        // Collect usage statistics for placed boxes.
        const loggedId = (meta && meta.categoryId !== 'Custom boxes') ? operationId : 'custom';
        util.logUsage('box added', { operationId: loggedId });
        return box;
      },

      _deleteBox: function(boxId) {
        this.state.boxes = this.state.boxes.filter(function(box) {
          return box.id !== boxId;
        });
        this.state.boxes.map(function(box) {
          const inputs = box.inputs;
          for (let inputName in inputs) {
            if (inputs[inputName].boxId === boxId) {
              delete box.inputs[inputName];
            }
          }
        });
        this._build();
      },

      deleteBoxes: function(boxIds) {
        for (let i = 0; i < boxIds.length; i += 1) {
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
        const plugs = {};
        plugs[plug1.direction] = plug1;
        plugs[plug2.direction] = plug2;
        const src = plugs.outputs;
        const dst = plugs.inputs;

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
        const knownStateIds = [];
        this.stateId2Plug = {};
        for (let i = 0; i < stateInfo.length; i++) {
          const item = stateInfo[i];
          const boxOutput = item.boxOutput;
          const stateId = item.stateId;
          knownStateIds.push(stateId);
          const box = this.boxMap[boxOutput.boxId];
          const plug = box.outputMap[boxOutput.id];
          plug.stateId = stateId;
          plug.setHealth(item.success);
          plug.kind = item.kind;
          if (this.stateId2Plug[stateId] === undefined) {
            this.stateId2Plug[stateId] = [];
          }
          this.stateId2Plug[stateId].push(plug);
        }
        longPoll.setStateIds(knownStateIds);
      },

      _assignSummaryInfoToBoxes: function(summaries) {
        for (let i = 0; i < this.boxes.length; i++) {
          const box = this.boxes[i];
          box.summary = summaries[box.instance.id];
          if (!box.summary) {
            box.summary = box.metadata.operationId;
          }
        }
      },

      updateProgress: function(progressMap) {
        for (let stateId in progressMap) {
          const progress = progressMap[stateId];
          const plugs = this.stateId2Plug[stateId] || [];
          for (let plug of plugs) {
            if (progress.filter(p => 0 < p && p < 1).length) {
              plug.updateProgress('in-progress');
            } else if (progress.filter(p => p < 0).length) {
              plug.updateProgress('error');
            } else if (progress.filter(p => p === 0).length) {
              plug.updateProgress('not-complete');
            } else {
              plug.updateProgress('complete');
            }
          }
        }
      },

      clearProgress: function() {
        for (let i = 0; i < this.boxes.length; i++) {
          const box = this.boxes[i];
          for (let j = 0; j < box.outputs.length; j++) {
            box.outputs[j].clearProgress();
          }
        }
      },

      _setBoxParams: function(boxId, plainParamValues, parametricParamValues) {
        this.boxMap[boxId].instance.parameters = plainParamValues;
        this.boxMap[boxId].instance.parametricParameters = parametricParamValues;
      },

      updateBox: function(id, plainParamValues, parametricParamValues) {
        const box = this.boxMap[id].instance;
        if (!angular.equals(plainParamValues, box.parameters) ||
          !angular.equals(parametricParamValues, box.parametricParameters)) {
          this._setBoxParams(id, plainParamValues, parametricParamValues);
          this.saveWorkspace();
        }
      },

      pasteFromData: function(boxes, currentPosition) {
        let minX = Infinity;
        let minY = Infinity;
        let box;
        for (let i = 0; i < boxes.length; ++i) {
          box = boxes[i];
          if (box.x < minX) { minX = box.x; }
          if (box.y < minY) { minY = box.y; }
        }
        const mapping = {};
        const newBoxes = [];
        for (let i = 0; i < boxes.length; ++i) {
          box = boxes[i];
          const createdBox = this._addBox(
            box.operationId,
            currentPosition.x + box.x - minX,
            currentPosition.y + box.y - minY);
          createdBox.parameters = box.parameters;
          createdBox.parametricParameters = box.parametricParameters;
          mapping[box.id] = createdBox;
          newBoxes.push(createdBox);
        }
        for (let i = 0; i < boxes.length; ++i) {
          const oldBox = boxes[i];
          const newBox = mapping[oldBox.id];
          for (const [key, oldInput] of Object.entries(oldBox.inputs)) {
            const newInput = mapping[oldInput.boxId];
            if (newInput) {
              newBox.inputs[key] = { boxId: newInput.id, id: oldInput.id };
            }
          }
        }
        this.saveWorkspace();
        return newBoxes;
      },

      saveIfBoxesDirty: function() {
        for (let i = 0; i < this.boxes.length; i++) {
          if (this.boxes[i].isDirty) {
            return this.saveWorkspace();
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
        const that = this;
        that.loadWorkspace(util.post('/ajax/undoWorkspace', that.ref()));
      },

      redo: function() {
        if (!this.canRedo()) { return; }
        const that = this;
        that.loadWorkspace(util.post('/ajax/redoWorkspace', that.ref()));
      },

      startCustomBoxSavingAs: function() {
        const path = this.name;
        this.saveCustomBoxAsName =
        path.substr(0, path.lastIndexOf('/') + 1) + 'custom_boxes/nameOfCustomBox';
      },

      saveAsCustomBox: function(ids, name, description) {
        const workspaceParameters =
        JSON.parse(this.boxMap['anchor'].instance.parameters.parameters || '[]');
        const boxes = [{
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
        const inputNameCounts = {};
        const outputNameCounts = {};
        const SEPARATOR = ', ';
        const PADDING_X = 200;
        const PADDING_Y = 150;
        let inputBoxY = PADDING_Y;
        let outputBoxY = PADDING_Y;
        const internallyUsedOutputs = {};
        const externallyUsedOutputCounts = {};
        // This custom box will replace the selected boxes.
        const customBox = {
          id: this.getUniqueId(name),
          operationId: name,
          x: 0,
          y: 0,
          inputs: {},
          parameters: {},
          parametricParameters: {},
        };
        // Pass all workspace parameters through.
        for (let i = 0; i < workspaceParameters; ++i) {
          const param = workspaceParameters[i].id;
          customBox.parametricParameters[param] = '$' + param;
        }
        let minX = Infinity;
        let minY = Infinity;
        let maxX = -Infinity;
        for (let i = 0; i < ids.length; ++i) {
          const box = this.boxMap[ids[i]];
          // Place the custom box in the average position of the selected boxes.
          customBox.x += box.instance.x / ids.length;
          customBox.y += box.instance.y / ids.length;
          minX = Math.min(minX, box.instance.x);
          minY = Math.min(minY, box.instance.y);
          maxX = Math.max(maxX, box.instance.x);
        }
        // Record used outputs.
        for (let i = 0; i < this.boxes.length; ++i) {
          const box = this.boxes[i];
          const inputs = Object.keys(box.instance.inputs);
          for (let j = 0; j < inputs.length; ++j) {
            const input = box.instance.inputs[inputs[j]];
            if (input.boxId) {
            /* eslint-disable no-console */
              console.assert(!input.boxId.includes(SEPARATOR) && !input.id.includes(SEPARATOR));
              const key = input.boxId + SEPARATOR + input.id;
              externallyUsedOutputCounts[key] = (externallyUsedOutputCounts[key] || 0) + 1;
            }
          }
        }
        for (let i = 0; i < ids.length; ++i) {
          const box = this.boxMap[ids[i]];
          // "input-" and "output-" IDs will be used for the input and output boxes.
          /* eslint-disable no-console */
          console.assert(box.instance.id.indexOf('input-') !== 0);
          console.assert(box.instance.id.indexOf('output-') !== 0);
          if (ids[i] === 'anchor') { continue; } // Ignore anchor.
          // Copy this box to the new workspace.
          const instance = angular.copy(box.instance);
          boxes.push(instance);
          instance.x += PADDING_X - minX;
          instance.y += PADDING_Y / 2 - minY;
          for (let j = 0; j < box.inputs.length; ++j) {
            const inputName = box.metadata.inputs[j];
            const input = box.instance.inputs[inputName];
            const inputBoxId = input && input.boxId;
            // Record internally used output.
            if (inputBoxId) {
            /* eslint-disable no-console */
              console.assert(!input.boxId.includes(SEPARATOR) && !input.id.includes(SEPARATOR));
              externallyUsedOutputCounts[input.boxId + SEPARATOR + input.id] -= 1;
              internallyUsedOutputs[input.boxId + SEPARATOR + input.id] = true;
            }
            // Create input box if necessary.
            if (!ids.includes(inputBoxId)) {
              let inputBoxName = inputName;
              let inputNameCount = inputNameCounts[inputName] || 0;
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
              if (inputBoxId) { // Connected to a non-selected box.
                customBox.inputs[inputBoxName] = input;
              }
            }
          }
        }
        // Add output boxes as necessary.
        for (let i = 0; i < ids.length; ++i) {
          const box = this.boxMap[ids[i]];
          for (let j = 0; j < box.metadata.outputs.length; ++j) {
            const outputName = box.metadata.outputs[j];
            if (!internallyUsedOutputs[box.instance.id + SEPARATOR + outputName] ||
              externallyUsedOutputCounts[box.instance.id + SEPARATOR + outputName] > 0) {
              let outputBoxName = outputName;
              const outputNameCount = outputNameCounts[outputName] || 0;
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
              for (let k = 0; k < this.arrows.length; ++k) {
                const arrow = this.arrows[k];
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
        delete this._boxCatalogMap; // Force a catalog refresh.
        const that = this;
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
            that.saveWorkspace();
          }, function error(err) {
            that.loadWorkspace();
            throw err;
          }),
        };
      },

      startSavingAs: function() {
        this.showSaveAs = true;
        this.saveAsName = this.name;
      },

      maybeSaveAs: function(name, done) {
        // We only need to do an actual action if the user has changed the name.
        if (name !== this.name) {
          this.saveAs(name).finally(done).then(() => this.showSaveAs = false);
        } else {
          done();
          this.showSaveAs = false;
        }
      },

      saveAs: function(newName) {
        return util.post('/ajax/forkEntry',
          {
            from: this.name,
            to: newName,
          }).then(function() {
          window.location = '#/workspace/' + newName;
        });
      },

      deleteArrow: function(arrow) {
        this.arrows = this.arrows.filter(function(a) { return a !== arrow; });
        delete arrow.dst.boxInstance.inputs[arrow.dst.id];
        this.saveWorkspace();
      },

    };

    return WorkspaceWrapper;
  });
