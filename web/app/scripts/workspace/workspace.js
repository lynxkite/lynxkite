'use strict';

// This class manages a workspace state and its connection to Angular
// components (workspace-drawing-board, box-editor, state-view) and the
// backend.
// Handling the workspace state data structure and wrapping it with API
// objects is outsourced from here to workspaceWrapper.
//
// Life cycle:
// 1. boxCatalog needs to be loaded at all times for things to work
// 2. loadWorkspace()
//    - downloads a workspace state and saves it in backendState
//    - creates a workspaceWrapper using the downloaded state and
//      sets this.wrapper to point to it
//    - visible GUI gets updated based on this.wrapper via
//      workspace-drawing-board
// 3. user edit happens, e.g. box move, add box, or add arrow
//    - this updates the wrapper.state
//    - all frontend-facing objects are updated inside
//      workspaceWrapper
//    - backendState remains unchanged at this point
// 5. saveWorkspace()
// 6. GOTO 2

angular.module('biggraph')
  .factory('workspace', function(workspaceWrapper, util, $interval) {
    return function(boxCatalog, workspaceName) {
      var progressUpdater;

      var boxCatalogMap = {};
      for (var i = 0; i < boxCatalog.boxes.length; ++i) {
        var boxMeta = boxCatalog.boxes[i];
        boxCatalogMap[boxMeta.operationID] = boxMeta;
      }

      var workspace = {
        name: workspaceName,

        boxes: function() {
          return this.wrapper ? this.wrapper.boxes : [];
        },

        arrows: function() {
          return this.wrapper ? this.wrapper.arrows : [];
        },

        loadWorkspace: function() {
          var that = this;
          util.nocache(
            '/ajax/getWorkspace',
            {
              name: this.name
            })
            .then(function(response) {
              var state = response.workspace;
              that.backendState = state;
              // User edits will be applied to a deep copy of
              // the original backend state. This way watchers
              // of backendState will only be notified once the
              // backend is fully aware of the new state.
              var stateCopy = angular.copy(state);
              that.wrapper = workspaceWrapper(
                stateCopy, boxCatalogMap);
              that.wrapper.assignStateInfoToPlugs(response.outputs);
            })
            .then(function() {
              that.startProgressUpdate();
            });
        },

        saveWorkspace: function() {
          var that = this;
          util.post(
            '/ajax/setWorkspace',
            {
              name: this.name,
              workspace: that.wrapper.state,
            }).finally(
              // Reload workspace both in error and success cases.
              function() { that.loadWorkspace(); });
        },

        selectBox: function(boxId) {
          this.selectedBoxId = boxId;
        },

        selectedBox: function() {
          if (this.selectedBoxId) {
            return this.wrapper.boxMap[this.selectedBoxId];
          } else {
            return undefined;
          }
        },

        updateSelectedBox: function(paramValues) {
          this.wrapper.setBoxParams(this.selectedBoxId, paramValues);
          this.saveWorkspace();
        },

        selectState: function(boxID, outputID) {
          var outPlug = this.wrapper.boxMap[boxID].outputMap[outputID];
          this.selectedStateId = outPlug.stateID;
          this.selectedStateKind = outPlug.kind;
        },

        selectPlug: function(plug) {
          this.selectedPlug = plug;
          if (plug.direction === 'outputs') {
            this.selectState(plug.boxId, plug.data.id);
          } else {
            this.selectedState = undefined;
          }
        },

        onMouseMove: function(mouseLogical) {
          this.mouseLogical = mouseLogical;
          if (event.buttons === 1 && this.movedBox) {
            this.movedBox.onMouseMove(this.mouseLogical);
          }
        },

        onMouseUp: function() {
          if (this.movedBox && this.movedBox.isMoved) {
            this.saveWorkspace();
          }
          this.movedBox = undefined;
          this.pulledPlug = undefined;
        },

        onMouseDownOnBox: function(box, mouseLogical) {
          this.selectBox(box.instance.id);
          this.movedBox = box;
          this.movedBox.onMouseDown(mouseLogical);
        },

        onMouseDownOnPlug: function(plug, event) {
          event.stopPropagation();
          this.pulledPlug = plug;
        },

        onMouseUpOnPlug: function(plug, event) {
          event.stopPropagation();
          if (this.pulledPlug) {
            var otherPlug = this.pulledPlug;
            this.pulledPlug = undefined;
            if (this.wrapper.addArrow(otherPlug, plug)) {
              this.saveWorkspace();
            }
          }
          if (!this.pulledPlug || this.pulledPlug !== plug) {
            this.selectPlug(plug);
          }
        },

        // boxID should be used for test-purposes only
        addBox: function(operationId, pos, boxID) {
          this.wrapper.addBox(operationId, pos.x, pos.y, boxID);
          this.saveWorkspace();
        },

        getAndUpdateProgress: function(errorHandler) {
          var that = this;
          var wrapperBefore = this.wrapper;
          if (wrapperBefore) {
            util.nocache('/ajax/getProgress', {
              stateIDs: wrapperBefore.knownStateIDs,
            }).then(
              function success(progressMap) {
                if (that.wrapper && that.wrapper === wrapperBefore) {
                  that.wrapper.updateProgress(progressMap);
                }
              },
              errorHandler);
          }
        },

        startProgressUpdate: function() {
          var that = this;
          this.stopProgressUpdate();
          progressUpdater = $interval(function() {
            function errorHandler(error) {
              util.error('Couldn\'t get status information.', error);
              that.stopProgressUpdate();
              that.wrapper.clearProgress();
            }
            that.getAndUpdateProgress(errorHandler);
          }, 2000);
        },

        stopProgressUpdate: function() {
          if (progressUpdater) {
            $interval.cancel(progressUpdater);
            progressUpdater = undefined;
          }
        },
      };

      workspace.loadWorkspace();
      return workspace;
    };
  });
